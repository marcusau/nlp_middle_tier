#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os,pathlib,sys
sys.path.append(os.getcwd())

parent_path = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(parent_path))

master_path = parent_path.parent
sys.path.append(str(master_path))

project_path = master_path.parent
sys.path.append(str(project_path))


Config_Folder_path=pathlib.Path(__file__).parent


yaml_filepath=Config_Folder_path/'yamls'/"API_Config.yaml"

from Config.object_generator.API import Config

Info = Config.from_yaml(yaml_filepath.as_posix())