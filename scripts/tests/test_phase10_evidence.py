import copy
import importlib.util
import json
import pathlib
import sys
import tempfile
import unittest
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import phase10_evidence as evidence


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.session = str(uuid.uuid4())
        self.samples = [dict(session=self.session, platform='macos-arm64-v1', host='host', kernel='27.0.0',
            commands=evidence.COMMANDS, start_ns=1 + i * 12_000_000_000,
            end_ns=1_000_000_001 + i * 12_000_000_000,
            time_ns=1_800_000_000_000_000_000 + i * 12_000_000_000,
            readings=dict(top='CPU usage: 10.0% user, 10.0% sys, 80.0% idle',
                uptime='12:00 up 2 days, load averages: 2.0 2.1 2.2',
                vm='Mach Virtual Memory Statistics: (page size of 16384 bytes)\nSwapouts: 123.',
                processes='    1 /sbin/launchd\n  100 /usr/bin/python3\n 200 (git)\n 300 -zsh')) for i in range(3)]

    def check(self, samples=None):
        return evidence.check_readiness(samples or self.samples, self.session, 'host')

    def test_valid_and_frozen_boundaries(self):
        self.check()
        self.samples[-1]['readings']['vm'] = self.samples[-1]['readings']['vm'].replace('123.', '1123.')
        self.check()
        self.samples[-1]['readings']['vm'] = self.samples[-1]['readings']['vm'].replace('1123.', '1124.')
        with self.assertRaises(evidence.VerificationError): self.check()

    def test_missing_stale_malformed_and_fabricated_readiness(self):
        mutations = [
            lambda s: s.pop(),
            lambda s: s[1].update(start_ns=s[0]['end_ns'] + 1),
            lambda s: s[1].update(time_ns=s[0]['time_ns']),
            lambda s: s[0].update(start_ns='yesterday'),
            lambda s: s[0].update(host=''),
            lambda s: s[0].update(platform='linux'),
            lambda s: s[0].update(session=str(uuid.uuid4())),
            lambda s: s[0].update(commands={}),
            lambda s: s[0]['readings'].update(processes='no competitors'),
            lambda s: s[0]['readings'].update(processes=' 200 /tmp/ade-worker'),
            lambda s: s[0]['readings'].update(processes=' 200 (cargo)'),
            lambda s: s[0]['readings'].update(processes=' 200 /tmp/ade-worker pretending-to-be-idle'),
            lambda s: s[0]['readings'].update(top='80% idle'),
            lambda s: s[0]['readings'].update(uptime='no load'),
            lambda s: s[0]['readings'].update(vm='Swapouts: 0.'),
            lambda s: s[0]['readings'].update(top='CPU usage: 69.9% idle'),
            lambda s: s[0]['readings'].update(uptime='load average: 8.01'),
        ]
        for mutate in mutations:
            s = copy.deepcopy(self.samples)
            mutate(s)
            with self.subTest(mutate=mutate), self.assertRaises(evidence.VerificationError): self.check(s)

    def test_legacy_readiness_cannot_be_promoted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            evidence.write(root, 'readiness.json', self.samples)
            with self.assertRaisesRegex(evidence.VerificationError, 'v2 release seal missing'):
                evidence.check_release(root)


class NegativeTests(unittest.TestCase):
    def test_reached_faults_and_forged_stages(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            evidence.write(root, 'uniform-manifest-stdout.log', dict(files=[dict(relative_path='part.parquet',
                bytes=100, fingerprint='a' * 64)]))
            for kind, stage, options in [
                ('exit', 'exit_after_frame', ['scheduler_research_exit_after_emit_worker=0']),
                ('expiry', 'expired_live_lease', ['scheduler_research_no_heartbeat_worker=0',
                    'scheduler_lease_ms=80', 'scheduler_heartbeat_ms=20']),
                ('notice', 'spot_deadline', ['scheduler_research_delay_worker=0', 'scheduler_participation=' +
                    json.dumps([dict(after_completed=1, worker=0, action=dict(spot_notice=dict(grace_ms=1)))])]),
            ]:
                stem = 'negative-' + kind
                command = ['bench', 'validate', '--dataset', '/data', '--spec', 'spec',
                    *sum((['--engine-option', opt] for opt in options), [])]
                evidence.write(root, stem + '-command.json', command)
                (root / (stem + '-stdout.log')).write_text('')
                record = dict(schema_version=1, stage=stage, process_id=1, at_ms=100,
                    granted_ms=1, expires_ms=81, renewals=0, notice_ms=90, deadline_ms=91, completed=1,
                    emitted_rows=1, emitted_frames=1, fence=dict(execution=str(uuid.uuid4()), job=0,
                        epoch=1, digest='d' * 64), lease=dict(worker=0, nonce=1,
                        morsel=dict(id=0, path='/data/part.parquet', bytes=100, fingerprint='a' * 64)))
                def stderr(r):
                    (root / (stem + '-stderr.log')).write_text('ADE_SCHEDULER_FAULT ' + json.dumps(r) +
                        '\nambiguous expired source lease; spot notice expired before producer drain\n')
                stderr(record)
                evidence.check_negative(root, stem, 1)
                for field, value in [('stage', 'preflight'), ('at_ms', 0), ('process_id', 0)]:
                    forged = copy.deepcopy(record)
                    forged[field] = value
                    stderr(forged)
                    with self.subTest(kind=kind, field=field), self.assertRaises(evidence.VerificationError):
                        evidence.check_negative(root, stem, 1)

    def test_preflight_errors_and_success_do_not_count_as_faults(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            for kind in ['exit', 'expiry', 'notice']:
                stem = 'negative-' + kind
                (root / (stem + '-stdout.log')).write_text('')
                (root / (stem + '-stderr.log')).write_text('worker binary not found')
                with self.assertRaisesRegex(evidence.VerificationError, 'fault evidence'):
                    evidence.check_negative(root, stem, 1)

                with self.assertRaisesRegex(evidence.VerificationError, 'succeeded'):
                    evidence.check_negative(root, stem, 0)
                (root / (stem + '-stdout.log')).write_text('{"validated": true}')
                with self.assertRaisesRegex(evidence.VerificationError, 'stdout'):
                    evidence.check_negative(root, stem, 1)

class SealedArtifactTests(ReadinessTests):
    def fixture(self, root):
        item = dict(stem='uniform-2-static')
        evidence.write(root, 'inventory.json', [item])
        evidence.write(root, 'source-identity.json', dict(source_commit='commit'))
        evidence.write(root, 'environment-stdout.log', dict(environment=dict(os='macos', arch='aarch64', kernel='27.0.0'),
            repository=dict(git_commit='commit', git_dirty=False)))
        evidence.write(root, 'readiness.json', self.samples)
        post = copy.deepcopy(self.samples[-1])
        post.update(start_ns=50_000_000_000, end_ns=51_000_000_000, time_ns=1_800_000_050_000_000_000)
        evidence.write(root, 'postflight.json', post)
        names = ['statuses.json', 'binary-sha256.json', 'source.tar', 'source.bundle', 'declaration.md']
        names += [item['stem'] + suffix for suffix in ['-result.json', '-metrics.jsonl', '-command.json', '-stdout.log', '-stderr.log']]
        for regime in ['imbalanced', 'uniform']:
            for kind in ['manifest', 'oracle']:
                names += [f'{regime}-{kind}{suffix}' for suffix in ['-stdout.log', '-stderr.log', '-command.json']]
        for kind in ['exit', 'expiry', 'notice']:
            names += [f'negative-{kind}{suffix}' for suffix in ['-stdout.log', '-stderr.log', '-command.json']]
        commands = {}
        for name in names:
            evidence.write(root, name, {})
            if name.endswith('-command.json'):
                stem = name.removesuffix('-command.json')
                evidence.write(root, name, ['bench', stem])
                commands[stem] = dict(argv=['bench', stem], start_ns=30_000_000_000, end_ns=40_000_000_000)
        evidence.write(root, item['stem'] + '-result.json', dict(run_id='run'))
        evidence.seal(root, self.session, commands)
        seal = json.loads((root / 'release-evidence.json').read_text())
        seal['host'] = 'host'
        evidence.write(root, 'release-evidence.json', seal)

    def test_copied_altered_missing_readiness_and_result_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            self.fixture(root)
            evidence.check_release(root)
            for name in ['readiness.json', 'postflight.json', 'uniform-2-static-result.json']:
                original = (root / name).read_bytes()
                (root / name).write_bytes(original + b' ')
                with self.assertRaisesRegex(evidence.VerificationError, 'altered sealed'):
                    evidence.check_release(root)
                (root / name).write_bytes(original)
            (root / 'readiness.json').unlink()
            with self.assertRaises(evidence.VerificationError): evidence.check_release(root)

    def test_resealed_stale_readiness_and_foreign_postflight_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            self.fixture(root)
            seal = json.loads((root / 'release-evidence.json').read_text())
            for name, field, value in [('readiness.json', 'time_ns', 1), ('postflight.json', 'host', 'foreign')]:
                value_json = json.loads((root / name).read_text())
                if isinstance(value_json, list): value_json[1][field] = value
                else: value_json[field] = value
                evidence.write(root, name, value_json)
                seal['files'][name] = evidence.sha(root / name)
                evidence.write(root, 'release-evidence.json', seal)
                with self.assertRaises(evidence.VerificationError): evidence.check_release(root)

    def test_best_only_even_consistently_rewritten_summary_is_refused(self):
        spec = importlib.util.spec_from_file_location('p10verify', pathlib.Path(__file__).resolve().parents[1] / 'phase10-verify.py')
        verifier = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(verifier)
        for workers, count in [(2, 5), (4, 3)]:
            run = dict(run_config=dict(warmups=1, iterations=count, dataset_verification='full'), warmups=[{}], iterations=[{}] * count)
            verifier.check_repetitions(run, workers)
            run['iterations'].pop()
            with self.assertRaises(verifier.VerificationError): verifier.check_repetitions(run, workers)
            run['run_config']['iterations'] = 1
            run['iterations'] = [{}]
            with self.assertRaises(verifier.VerificationError): verifier.check_repetitions(run, workers)



class ResourceTests(unittest.TestCase):
    def test_hash_and_provisional_buffer_bounds_including_parked_producers(self):
        spec = importlib.util.spec_from_file_location('p10verify_resources', pathlib.Path(__file__).resolve().parents[1] / 'phase10-verify.py')
        verifier = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(verifier)
        for workers in [2, 3, 4]:
            metrics = dict(admission_hash_threads=8, admission_hash_scratch_peak_bytes=2 * 1024 * 1024,
                producers={str(w): dict(source_files=1 if w else 0, source_hash_threads=8 // workers,
                    source_hash_scratch_peak_bytes=1024 * 1024 if w else 0,
                    routing_buffer_capacity_rows=workers * 1024, routing_buffer_peak_rows=100 if w else 0)
                    for w in range(workers)})
            verifier.check_resources(metrics, workers)
            mutations = [
                lambda m: m.update(admission_hash_threads=9),
                lambda m: m.update(admission_hash_scratch_peak_bytes=2 * 1024 * 1024 + 1),
                lambda m: m['producers']['1'].update(source_hash_threads=9),
                lambda m: m['producers']['0'].update(source_hash_scratch_peak_bytes=1024 * 1024),
                lambda m: m['producers']['1'].update(routing_buffer_peak_rows=workers * 1024 + 1),
            ]
            for mutate in mutations:
                bad = copy.deepcopy(metrics)
                mutate(bad)
                with self.assertRaises(verifier.VerificationError): verifier.check_resources(bad, workers)


if __name__ == '__main__':
    unittest.main()


class ComparatorTests(unittest.TestCase):
    def test_pipeline_labels_cannot_promote_legacy_or_leased_as_projected_static(self):
        spec = importlib.util.spec_from_file_location('p10verify_comparator', pathlib.Path(__file__).resolve().parents[1] / 'phase10-verify.py')
        verifier = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(verifier)
        legacy = dict(scheduler_policy='static')
        projected = dict(legacy, producer_pipeline='static_projected')
        verifier.check_pipeline(legacy, 'uniform-4-static')
        verifier.check_pipeline(projected, 'uniform-4-static_projected')
        for config, stem in [(legacy, 'uniform-4-static_projected'),
                (projected, 'uniform-4-static'),
                (dict(projected, scheduler_policy='leased_v1'), 'uniform-4-static_projected')]:
            with self.assertRaises(verifier.VerificationError): verifier.check_pipeline(config, stem)

    def test_frozen_repetition_count_rejects_best_only_and_short_confirmation(self):
        spec = importlib.util.spec_from_file_location('p10verify_repetitions', pathlib.Path(__file__).resolve().parents[1] / 'phase10-verify.py')
        verifier = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(verifier)
        for workers, count in [(2, 5), (4, 3)]:
            run = dict(run_config=dict(warmups=1, iterations=count, dataset_verification='full'),
                       warmups=[{}], iterations=[{} for _ in range(count)])
            verifier.check_repetitions(run, workers)
            run['iterations'].pop()
            with self.assertRaises(verifier.VerificationError): verifier.check_repetitions(run, workers)
