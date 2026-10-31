"""Owned disposable service fixture. Never accesses existing topics or buckets."""
import base64,hashlib,json,os,pathlib,shutil,socket,subprocess,time,uuid

def require(ok,why):
    if not ok:raise ValueError(why)
def dump(path,value):pathlib.Path(path).write_text(json.dumps(value,indent=2,sort_keys=True))
def rows(path):
    if not pathlib.Path(path).exists():return []
    return [json.loads(line) for line in pathlib.Path(path).read_text().splitlines() if line.startswith('{')]
def port():
    with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def private(path):path.mkdir(mode=0o700);return path

class Services:
    @classmethod
    def start(cls,args,root):
        obj=cls.__new__(cls);obj.children=[]
        try:obj.__init__(args,root)
        except BaseException:
            obj.close();raise
        return obj
    def __init__(self,args,root):
        self.args=args;self.root=root;self.children=[];self.commands=[];self.ledgers={};self.repo=pathlib.Path(__file__).resolve().parents[2]
        self.cluster=base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip('=');self.port=port();self.address=f'127.0.0.1:{self.port}';control=port()
        self.java=str(args.java_home/'bin/java');self.cp=str(args.kafka_home/'libs/*');self.fixture=private(root/'fixture')
        self.call([str(args.java_home/'bin/javac'),'-cp',self.cp,'-d',str(self.fixture),str(self.repo/'scripts/phase15/Fixture.java')],'javac')
        self.fixture_cp=str(self.fixture)+os.pathsep+self.cp
        p=root/'broker.properties';p.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:{control}
listeners=PLAINTEXT://127.0.0.1:{self.port},CONTROLLER://127.0.0.1:{control}
advertised.listeners=PLAINTEXT://127.0.0.1:{self.port}
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs={root/'broker-data'}
num.partitions=2
offsets.topic.replication.factor=1
offsets.topic.num.partitions=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
transaction.state.log.num.partitions=1
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
message.max.bytes=32768
log.retention.hours=24
''')
        self.call([self.java,'-cp',self.cp,'kafka.tools.StorageTool','format','--cluster-id',self.cluster,'--config',str(p)],'format')
        broker=self.spawn([self.java,'-Xmx512m','-Xms256m','-cp',self.cp,'kafka.Kafka',str(p)],'broker');self.ready(self.port,broker);time.sleep(2)
        require(self.admin('cluster','unused').decode().strip()==self.cluster,'cluster mismatch')
        self.minio_port=port();console=port();self.proxy_port=port();self.endpoint=f'http://127.0.0.1:{self.proxy_port}'
        self.env=dict(os.environ,AWS_ACCESS_KEY_ID='ade17'+uuid.uuid4().hex,AWS_SECRET_ACCESS_KEY=uuid.uuid4().hex+uuid.uuid4().hex)
        env=dict(self.env,MINIO_ROOT_USER=self.env['AWS_ACCESS_KEY_ID'],MINIO_ROOT_PASSWORD=self.env['AWS_SECRET_ACCESS_KEY'],MINIO_BROWSER='off')
        mini=self.spawn([str(args.minio),'server','--address',f'127.0.0.1:{self.minio_port}','--console-address',f'127.0.0.1:{console}',str(root/'minio-data')],'minio',env);self.ready(self.minio_port,mini)
        provision="""import boto3,os,sys
from botocore.config import Config
s=boto3.client('s3',endpoint_url=sys.argv[1],region_name='us-east-1',aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],config=Config(proxies={},retries={'max_attempts':0}))
s.create_bucket(Bucket='ade-phase16')
"""
        self.call([str(args.service_python),'-c',provision,f'http://127.0.0.1:{self.minio_port}'],'bucket',env=self.env)
        proxy=self.spawn([str(args.service_python),str(self.repo/'scripts/phase16/service.py'),'--backend',f'http://127.0.0.1:{self.minio_port}','--port',str(self.proxy_port),'--root',str(root)],'proxy');self.ready(self.proxy_port,proxy)
        dump(root/'services.json',dict(cluster=self.cluster,address=self.address,kafka='3.9.1',java_sha256=hashlib.sha256(pathlib.Path(self.java).read_bytes()).hexdigest(),minio_sha256=hashlib.sha256(args.minio.read_bytes()).hexdigest(),minio_pid=mini.pid,broker_pid=broker.pid,proxy_pid=proxy.pid,endpoint=self.endpoint,java_version=self.call([self.java,'-version'],'java-version').decode(errors='replace')))
    def call(self,cmd,label,data=None,env=None,expected=0,timeout=40):
        self.commands.append(dict(label=label,argv=list(map(str,cmd))));dump(self.root/'commands.json',self.commands)
        r=subprocess.run(cmd,input=data,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,timeout=timeout)
        (self.root/(label+'.stdout')).write_bytes(r.stdout);(self.root/(label+'.stderr')).write_bytes(r.stderr)
        require(r.returncode==expected,f'{label} exit {r.returncode}: {r.stderr[-1500:]}');return r.stdout
    def spawn(self,cmd,label,env=None,stdout=None):
        self.commands.append(dict(label=label,argv=list(map(str,cmd))));dump(self.root/'commands.json',self.commands)
        out=open(self.root/(label+'.stdout'),'wb') if stdout is None else stdout;err=open(self.root/(label+'.stderr'),'wb')
        p=subprocess.Popen(cmd,stdout=out,stderr=err,env=env);self.children.append(p)
        if stdout is None:out.close()
        err.close();return p
    def ready(self,port,p):
        until=time.monotonic()+30
        while time.monotonic()<until:
            require(p.poll() is None,'service exited')
            try:
                with socket.create_connection(('127.0.0.1',port),timeout=.1):return
            except OSError:time.sleep(.1)
        raise ValueError('service start timeout')
    def admin(self,mode,topic,extra=(),data=None):return self.call([self.java,'-cp',self.fixture_cp,'Fixture',self.address,mode,topic,*map(str,extra)],f'admin-{len(self.commands):04}-{mode}',data)
    def topic(self,label,parts=2):
        topic='ade17-'+uuid.uuid4().hex[:10]+'-'+label;self.admin('create',topic,[parts]);self.ledgers[topic]=[];return topic
    def produce(self,topic,events):
        data=''.join(str(p)+'\t'+json.dumps(e,separators=(',',':'))+'\n' for p,e in events).encode()
        out=self.admin('produce',topic,data=data)
        result=[dict(partition=int(p),offset=int(off),event=json.loads(body)) for p,off,body in (line.split('\t',2) for line in out.decode().splitlines())]
        self.ledgers[topic]+=result;dump(self.root/'ledgers.json',self.ledgers);return result
    def close(self):
        for p in reversed(self.children):
            if p.poll() is None:
                p.terminate()
                try:p.wait(5)
                except subprocess.TimeoutExpired:p.kill();p.wait()
