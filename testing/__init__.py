import unittest

from attila.configurations import get_attila_config_manager
from attila.fs import Path


class TestPlugin(unittest.TestCase):

    def test_config_loader(self):
        ip = '192.4.27.47'
        user = 'drp_cnda_rnamops_red'
        bare_path = '/home/dropboxes/cnda/rnamops/red'

        config_manager = get_attila_config_manager()
        base_path = config_manager.load_path('sftp://{user}@{ip}{bare_path}'.format(
            user=user,
            ip=ip,
            bare_path=bare_path
        ))

        local_test_file = Path('./test_files/test_file1.txt')
        test_results_folder = Path('./test_results')

        print("Opening connections...")
        with base_path.connection, local_test_file.connection:
            test_results_folder.make_dir(overwrite=True, clear=True, fill=True)
            self.assertTrue(test_results_folder.exists)
            self.assertTrue(test_results_folder.is_dir)

            self.assertTrue(local_test_file.exists)
            self.assertTrue(local_test_file.is_file)

            remote_test_folder = base_path['test']
            assert isinstance(remote_test_folder, Path)

            print("Creating directory...")
            remote_test_folder.make_dir(overwrite=True, clear=True, fill=True)
            self.assertTrue(remote_test_folder.exists)
            self.assertTrue(remote_test_folder.is_dir)

            remote_test_file = remote_test_folder[local_test_file.name]
            self.assertFalse(remote_test_file.exists)

            print("Copying file to remote...")
            local_test_file.copy_into(remote_test_folder, overwrite=True, clear=True, fill=True)
            self.assertTrue(remote_test_file.exists)
            self.assertTrue(remote_test_file.is_file)

            self.assertEqual(remote_test_folder.list(), [local_test_file.name])

            print("Copying file to local...")
            results_file = test_results_folder[local_test_file.name]
            remote_test_file.copy_into(test_results_folder, overwrite=True, clear=True, fill=True)
            self.assertTrue(results_file.exists)
            self.assertTrue(results_file.is_file)

            print("Reading original...")
            with local_test_file.open('rb') as file:
                original = file.read()

            print("Reading remote...")
            with remote_test_file.open('rb') as file:
                remote = file.read()

            print("Reading results...")
            with results_file.open('rb') as file:
                results = file.read()

            print("Comparing...")
            self.assertEqual(original, remote)
            self.assertEqual(original, results)

            print("Cleaning up...")
            remote_test_file.remove()
            self.assertFalse(remote_test_file.exists)
            self.assertEqual(len(remote_test_folder), 0)

            remote_test_folder.remove()
            self.assertFalse(remote_test_folder.exists)

            results_file.remove()
            self.assertFalse(results_file.exists)

            print("Done.")
