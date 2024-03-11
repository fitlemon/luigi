import luigi
import logging
import requests
import subprocess
import os
import tarfile
import io
import pandas as pd


logger = logging.getLogger('luigi-interface')


class DownloadDataset(luigi.Task):
    # Загрузка датасета из интернета
    link = 'https://www.ncbi.nlm.nih.gov/geo/download/?acc={dataset_name}&format=file'
    dataset_name = luigi.Parameter(default='GSE68849')
    TARGET_PATH = '/home/fitlemon/code/luigi/data'
    
    
    def output(self):
        return luigi.LocalTarget(f'{self.TARGET_PATH}/{self.dataset_name}.tar')
    
    def run(self):
        full_link = self.link.format(dataset_name=self.dataset_name)
        logger.info(f"Downloading archive from {full_link}...")
        response = requests.get(full_link)
        response.raise_for_status()
        logger.info(f'File downloaded from {full_link}')
        logger.info(f"Saving archive with name {self.dataset_name}.tar...")
        with open(self.output().path, 'wb') as f:
            f.write(response.content)
            logger.info(f'Saved archive to {self.output()}') 


class ExtractArchives(luigi.Task):
    # Извлечение архивов из главного архива .tar
    dataset_name = luigi.Parameter(default="GSE68849")

    def requires(self):
        return DownloadDataset(dataset_name=self.dataset_name)

    def run(self):
        logger.info(f"Creating directory {self.output()[0].path}...")
        os.makedirs(self.output()[0].path, exist_ok=True)  # Убедимся, что целевая папка существует
        logger.info(f"Extract files from tar archive into {self.output()[0].path}...")
        with tarfile.open(self.input().path) as archive:
            archive.extractall(path=self.output()[0].path)
        logger.info(f"Files from tar archive extracted into {self.output()[0].path}.")

    def output(self):
        if self.input():
            tar_file = self.input().path
            if os.path.exists(tar_file):
                tarf = tarfile.open(tar_file)
                files = tarf.getnames()
                return (luigi.LocalTarget(f"data/{self.dataset_name}/archives"), [luigi.LocalTarget(f"data/{self.dataset_name}/archives/{file_name}") for file_name in files])
        
class ExtractFiles(luigi.Task):
    # Извлечение файлов из архивов .gz
    dataset_name = luigi.Parameter(default="GSE68849")

    def requires(self):
        return ExtractArchives(dataset_name=self.dataset_name)
    
    def run(self):
        target_dir = self.output()[0].path
        os.makedirs(target_dir, exist_ok=True)
        input_files = [file.path for file in self.input()[1] if file.path.endswith('.gz')]
        output_files = [file.path for file in self.output()[1]]
        for input, output in zip(input_files, output_files):
            base_name = os.path.basename(input).rsplit('.', 2)[0]
            logger.info(f"Create folder for file {base_name}...")
            os.makedirs(os.path.join(target_dir, base_name), exist_ok=True)
            subprocess.run(['gunzip', '-c', input], stdout=open(output, 'wb'))
            logger.info(f"Unzipped archive to folder {base_name}.")
                    
    def output(self):
        if self.input():
            extracted_dir = self.input()[0].path
            print(extracted_dir) 
            parent_dir = os.path.dirname(extracted_dir)
            print(parent_dir)        
            target_dir = os.path.join(parent_dir, 'files')
            print(target_dir)  
            # Получить список сжатых gz файлов
            input_files = [file.path for file in self.input()[1]]
            output_files = []
            for file in input_files:
                if file.endswith('.gz'):
                    base_name = os.path.basename(file).rsplit('.', 2)[0]
                    target_path = os.path.join(target_dir, base_name, os.path.basename(file)[:-3])
                    # Сохраним название файла в архиве gz в список извлекаемых файлов
                    output_files.append(target_path)
            return (luigi.LocalTarget(target_dir), [luigi.LocalTarget(file_name) for file_name in output_files])
    
    
class SplitTables(luigi.Task):
    # Парсинг текстовых файлов и извлечение таблиц
    dataset_name = luigi.Parameter(default="GSE68849")

    def requires(self):
        return ExtractFiles(dataset_name=self.dataset_name)
    
    def run(self):
        input_files = [file.path for file in self.input()[1]]
        for file in input_files:
            logger.info(f"Split into tables the file {file}.")
            self._split_into_tables(file)
            
    def output(self):
        if self.input():
            tables = ['Columns.tsv', 'Controls.tsv', 'Heading.tsv', 'Probes.tsv'] # допустим я знаю, что должны быть извлечены именно эти таблицы с каждого файла
            input_files = [file.path for file in self.input()[1]]
            output_files = []
            for file in input_files:
                parent_dir = os.path.dirname(file)
                for table in tables:
                    output_file = os.path.join(parent_dir, table)
                    # Сохраним название файла-таблицы в список
                    output_files.append(output_file)
            return [luigi.LocalTarget(file) for file in output_files]
    
    def _split_into_tables(self, file_path):
        dfs = {}
        with open(file_path) as f:
            write_key = None
            fio = io.StringIO()
            for l in f.readlines():
                if l.startswith('['):
                    if write_key:
                        fio.seek(0)
                        header = None if write_key == 'Heading' else 'infer'
                        dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                         # Сохраняем DataFrame в файл
                        logger.info(f"Save dataframe {write_key}.")
                        self._save_dataframe(dfs[write_key], file_path, write_key)
                    fio = io.StringIO()
                    write_key = l.strip('[]\n')
                    continue
                if write_key: # Для последнего секции файла
                    fio.write(l)
            if write_key:
                fio.seek(0)
                dfs[write_key] = pd.read_csv(fio, sep='\t')
                # Сохраняем последний DataFrame
                self._save_dataframe(dfs[write_key], file_path, write_key)
            
    def _save_dataframe(self, df, original_file_path, table_name):
        # Создаем имя файла на основе оригинального файла и названия таблицы
        file_name = f"{table_name}.tsv"
        # Путь к папке, где лежит оригинальный файл
        dir_path = os.path.dirname(original_file_path)
        # Полный путь к новому файлу
        full_path = os.path.join(dir_path, file_name)
        # Создаем директорию, если не существует
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        # Сохраняем DataFrame
        df.to_csv(full_path, sep='\t', index=False)
    
    
class ProcessProbesTable(luigi.Task):
    # Обработка таблицы Probes и сохранения урезанной версии.
    dataset_name = luigi.Parameter(default="GSE68849")

    def requires(self):
        return SplitTables(dataset_name=self.dataset_name)

    def output(self):
        if self.input():
            input_files = [file.path for file in self.input()]
            input_dirs = set([os.path.dirname(file) for file in input_files])
            return [luigi.LocalTarget(os.path.join(dir, 'Probes_trimmed.tsv')) for dir in input_dirs]

    def run(self):
        input_files = [file.path for file in self.input()]
        for file in input_files:
            if os.path.basename(file) == 'Probes.tsv':
                logger.info(f"Modify Probes dataframe dataframe.")
                self._modify_df(file)
                    
    def _modify_df(self, file_path):
        df = pd.read_csv(file_path, sep='\t')
        df = df.drop(columns=['Definition', 'Ontology_Component', 'Ontology_Process', 'Ontology_Function', 'Synonyms', 'Obsolete_Probe_Id', 'Probe_Sequence'])
        new_file_path = str.replace(file_path, 'Probes', 'Probes_trimmed')
        df.to_csv(new_file_path, sep='\t', index=False)
        

class CheckAndClear(luigi.Task):
    # Удалить исходные файлы txt
    dataset_name = luigi.Parameter(default="GSE68849")
    
    def requires(self):
        return ProcessProbesTable(dataset_name = self.dataset_name)
    
    def run(self):
        input_files = [file.path for file in self.input()]
        dirs = [os.path.dirname(file) for file in input_files]
        for dir in dirs:
            for file in os.listdir(dir):
                if not file.endswith('.tsv'):
                    file_path = os.path.join(dir, file)
                    os.remove(file_path)
                    logger.info(f"File deleted: {file}")
                    

if __name__ == '__main__':
    luigi.run()
        