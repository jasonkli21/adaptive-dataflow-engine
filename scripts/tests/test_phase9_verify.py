import importlib.util
import copy
import json
import pathlib
import subprocess
import tempfile
import unittest
import uuid
import hashlib


SCRIPT = pathlib.Path(__file__).resolve().parents[1] / 'phase9-verify.py'
SPEC = importlib.util.spec_from_file_location('phase9_verify', SCRIPT)
verifier = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verifier)


class Phase9BindingTests(unittest.TestCase):
    def setUp(self):
        self.sources = [
            {
                'side': 'left',
                'files': [{
                    'path': '/data/tiny/left/part-0.parquet',
                    'rows': 10,
                    'bytes': 100,
                    'blake3': 'a' * 64,
                }],
            },
            {
                'side': 'right',
                'files': [{
                    'path': '/data/tiny/right/part-0.parquet',
                    'rows': 2,
                    'bytes': 50,
                    'blake3': 'b' * 64,
                }],
            },
        ]

    @staticmethod
    def make_arm(run_id, policy):
        config = {
            'policy': policy,
            'build': None,
            'local': {'hash_rows': 65536},
        }
        run = {'run_id': run_id}
        record = {
            'sidecar_version': 2,
            'run_id': run_id,
            'iteration_index': 0,
            'warmup': True,
            'process_id': 1001 if policy == 'forced_broadcast' else 1002,
            'metrics': {
                'spec': {
                    'config': config,
                    'execution': str(uuid.uuid4()),
                    'sources': [
                        {'side': s['side'], 'identity': 'c' * 64, 'files': s['files']}
                        for s in Phase9BindingTests.sources_fixture()
                    ],
                },
            },
        }
        return run, record, config

    @staticmethod
    def sources_fixture():
        return [
            {'side': 'left', 'files': [{
                'path': '/data/tiny/left/part-0.parquet', 'rows': 10,
                'bytes': 100, 'blake3': 'a' * 64,
            }]},
            {'side': 'right', 'files': [{
                'path': '/data/tiny/right/part-0.parquet', 'rows': 2,
                'bytes': 50, 'blake3': 'b' * 64,
            }]},
        ]

    def test_swapping_two_valid_tiny_arm_sidecars_fails_binding(self):
        run_a, sidecar_a, config_a = self.make_arm('run-a', 'forced_broadcast')
        run_b, sidecar_b, config_b = self.make_arm('run-b', 'observed_v1')
        seen_pids, seen_executions, seen_source_ids = set(), set(), {}
        verifier.check_join_record(
            sidecar_a, run_a, config_a, self.sources, 0, True,
            seen_pids, seen_executions, seen_source_ids,
        )
        verifier.check_join_record(
            sidecar_b, run_b, config_b, self.sources, 0, True,
            seen_pids, seen_executions, seen_source_ids,
        )
        with self.assertRaises(AssertionError):
            verifier.check_join_record(
                sidecar_a, run_b, config_b, self.sources, 0, True,
                seen_pids, seen_executions, seen_source_ids,
            )
        with self.assertRaises(AssertionError):
            verifier.check_join_record(
                sidecar_b, run_a, config_a, self.sources, 0, True,
                seen_pids, seen_executions, seen_source_ids,
            )

    def test_run_token_cannot_hide_a_different_effective_arm_config(self):
        run, sidecar, _ = self.make_arm('run-config', 'forced_broadcast')
        observed_config = dict(self.make_arm('other-run', 'observed_v1')[2])
        with self.assertRaises(AssertionError):
            verifier.check_join_record(
                sidecar, run, observed_config, self.sources, 0, True, set(), set(), {},
            )

    def test_same_rows_with_swapped_source_inventory_fails_binding(self):
        run, sidecar, config = self.make_arm('run-source', 'observed_v1')
        sidecar['metrics']['spec']['sources'].reverse()
        with self.assertRaises(AssertionError):
            verifier.check_join_record(
                sidecar, run, config, self.sources, 0, True, set(), set(), {},
            )

    def test_same_rows_with_changed_source_identity_fails_binding(self):
        run, first, config = self.make_arm('run-source-id', 'observed_v1')
        second = copy.deepcopy(first)
        second['process_id'] += 10
        second['metrics']['spec']['execution'] = str(uuid.uuid4())
        second['metrics']['spec']['sources'][0]['identity'] = 'd' * 64
        pids, executions, source_ids = set(), set(), {}
        verifier.check_join_record(
            first, run, config, self.sources, 0, True, pids, executions, source_ids,
        )
        with self.assertRaises(AssertionError):
            verifier.check_join_record(
                second, run, config, self.sources, 1, False, pids, executions, source_ids,
            )

    def test_source_archive_must_match_implementation_identity_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            source = b'pinned source archive'
            (root / 'source.tar').write_bytes(source)
            identity = {
                'schema_version': 1,
                'source_commit': 'commit-id',
                'runtime_tree': 'runtime-tree-id',
                'source_archive_sha256': hashlib.sha256(source).hexdigest(),
            }
            (root / 'source-identity.json').write_text(json.dumps(identity))
            verifier.check_source_identity(root, 'commit-id', 'runtime-tree-id')
            (root / 'source.tar').write_bytes(source + b' changed')
            with self.assertRaises(AssertionError):
                verifier.check_source_identity(root, 'commit-id', 'runtime-tree-id')

    def test_git_bundle_commit_tree_and_archive_are_bound_together(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            repository = root / 'repo'
            repository.mkdir()
            subprocess.run(['git', 'init', '-q', str(repository)], check=True)
            subprocess.run(['git', '-C', str(repository), 'config', 'user.name', 'ADE Test'], check=True)
            subprocess.run(['git', '-C', str(repository), 'config', 'user.email', 'ade-test@example.invalid'], check=True)
            (repository / 'crates').mkdir()
            (repository / 'crates' / 'source.txt').write_text('pinned runtime')
            subprocess.run(['git', '-C', str(repository), 'add', 'crates/source.txt'], check=True)
            subprocess.run(['git', '-C', str(repository), 'commit', '-qm', 'fixture'], check=True)
            commit = subprocess.check_output(
                ['git', '-C', str(repository), 'rev-parse', 'HEAD'], text=True
            ).strip()
            runtime = subprocess.check_output(
                ['git', '-C', str(repository), 'rev-parse', 'HEAD:crates'], text=True
            ).strip()
            archive = subprocess.check_output(
                ['git', '-C', str(repository), 'archive', '--format=tar', commit]
            )
            evidence = root / 'evidence'
            evidence.mkdir()
            (evidence / 'source.tar').write_bytes(archive)
            subprocess.run(
                ['git', '-C', str(repository), 'bundle', 'create',
                 str(evidence / 'source.bundle'), '--all', commit],
                check=True,
            )
            identity = {
                'schema_version': 1,
                'source_commit': commit,
                'runtime_tree': runtime,
                'source_archive_sha256': hashlib.sha256(archive).hexdigest(),
            }
            (evidence / 'source-identity.json').write_text(json.dumps(identity))
            (evidence / 'implementation-identity.json').write_text(json.dumps(identity))
            verifier.check_source_identity(evidence, commit, runtime)


if __name__ == '__main__':
    unittest.main()
