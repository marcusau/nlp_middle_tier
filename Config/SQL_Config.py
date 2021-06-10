
import os,pathlib,sys
sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))


Config_folder_path=pathlib.Path(__file__).parent

yaml_filepath=Config_folder_path/'yamls'/"SQL_Config.yaml"

from Config.object_generator.SQL import Config

Info = Config.from_yaml(yaml_filepath.as_posix())