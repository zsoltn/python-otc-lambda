#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""
Created Date:  June 11 2018
Author: Zsolt Nagy
Version: V1.1 ( Automotive - Lambda example  )
Copyright (c) 2018 T-Systems

 YAML Config Handler
 """

import yaml
import os
import csv


class Config:
    cfg = None

    def __init__(self, cfg_path=None, cfg_dict_for_testing=None):
        if cfg_path:  # production env
            self.__load_config_from_yaml(cfg_path)

        if cfg_dict_for_testing and not cfg_path:  # should be used only in test env
            self.cfg = cfg_dict_for_testing

        if (cfg_path is None) and (cfg_dict_for_testing is None):
            # in case nothing is given resort to the otc-lambda default yaml file
            self.__load_config_from_yaml(os.path.join(os.path.dirname(os.path.realpath(__file__)),"../conf/" , "config.yml"))


    def __load_config_from_yaml(self, config_file):
        with open(config_file, 'r') as yamlfile:
            self.cfg = yaml.load(yamlfile)

    def get_level1_value(self, section, key):
        try:            
            return self.cfg[section][key]
        except KeyError:
            return False
        except TypeError:
            return False

    def set_level1_value(self, section, key):
        try:            
            return self.cfg[section][key]
        except KeyError:
            return False
        except TypeError:
            return False

    """
    def server_public_url(self):        
        return self.get_level1_value( "defaults", "server_url")
    def work_position(self):        
        return self.get_level1_value( "defaults", "work_position")
    def employment_type(self):
        return self.get_level1_value( "defaults", "employment_type")
    def work_order(self):
        return self.get_level1_value( "defaults", "work_order")
    """
    """
    def csv_path(self):        
        return self.get_level1_value( "kafka_data_producer", "csv_path")
    def broker_list(self):        
        return self.get_level1_value( "kafka_data_producer", "broker_list")
    def topic(self):
        return self.get_level1_value( "kafka_data_producer", "topic")
    def cars(self):
        return self.get_level1_value( "kafka_data_producer", "cars")
    """

    def get_lambda_config(self,master,key):
        return self.get_level1_value(master, key) 



    def export_data_yaml( self,outfile,data ):
        with open(outfile, 'w') as outfile:
            yaml.dump(data, outfile, default_flow_style=False)
            
    def export_data_csv( self,outfile,data ):
        with open(outfile, 'w') as outfile:
            wr = csv.writer(outfile, quoting=csv.QUOTE_ALL)
            
            #[unicode(s).encode("utf-8") for s in data]
            flag=True
            
            wr.writerow(list(data[0].__dict__))
            for r in data:
                wr.writerow([unicode(s).encode("utf-8") for s in r.__dict__.values()])            



