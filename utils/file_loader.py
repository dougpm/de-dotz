import glob
import ntpath
from collections import namedtuple

class FileLoader:
    def load_files(self, directory: str, file_suffix: str) -> namedtuple:

        """"Reads all files within directory and returns a namedtuple containing files read"""
        
        self._directory = directory
        self._file_suffix = file_suffix
        self.file_names = self._get_file_names()
        self._file_tuple = self._create_file_tuple()
        self.loaded_files = self._file_tuple(*self._load_files_into_tuple())
        return self.loaded_files

    def _get_file_names(self) -> list:
        
        """Reads files with the specified suffix in the specified directory, returns a list with file names"""
        
        file_names = []
        for file_path in glob.glob(self._directory + "*"):
            if file_path.endswith(self._file_suffix):
                file_name = ntpath.basename(file_path)
                file_names.append(file_name)
        return file_names

    def _create_file_tuple(self):
        
        """Creates a nameduple to hold files read, returns it"""
        try:
            assert len(self.file_names) > 0
        except AssertionError:
            raise AssertionError("No files found in {0} with {1} suffix.".format(self._directory, self._file_suffix))
        self._file_tuple = namedtuple("FileHolder", self.file_names)
        return self._file_tuple
        
    def _load_files_into_tuple(self):

        """Reads files in the specified directory, yields files read"""
        
        for file in self.file_names:
            with open(self._directory + file) as f:
                yield f.read()
