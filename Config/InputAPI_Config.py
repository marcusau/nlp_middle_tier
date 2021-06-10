

import os,sys
from pathlib import Path

sys.path.append(os.getcwd())

parent_path = Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))


Config_folder_path=Path(__file__).parent

yaml_filepath=Config_folder_path/'yamls'/"InputAPI_Config.yaml"


from Config.object_generator.InputAPI import Config
Info = Config.from_yaml(yaml_filepath.as_posix())



