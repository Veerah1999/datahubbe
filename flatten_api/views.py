from django.http import JsonResponse

# from .code import flatten_file
# from .models import File

import json
import xml.etree.ElementTree as ET
import csv
from pyorc import Reader as ORCReader
from fastavro import reader as avro_reader
import pandas as pd

from rest_framework.views import APIView
from rest_framework.response import Response
from .serializers import FlattenFileSerializer


class FlattenFileView(APIView):
    def post(self, request,*args, **kwargs):
        import pdb
        pdb.set_trace()
        serializer = FlattenFileSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        input_path = serializer.validated_data['input_path']
        output_path = serializer.validated_data['output_path']
        #flatten_file(input_path, output_path)

        def read_json(file_path: str) -> list:
            try:
                with open(file_path, "r") as f:
                    data = json.load(f)
            except:
                raise Exception(f"Reading {file_path} file encountered an error")
            return data

        def normalize_json(data: list) -> list:
            new_data = []
            for obj in data:
                new_obj = {}
                for key, value in obj.items():
                    if not isinstance(value, dict):
                        new_obj[key] = value
                    else:
                        for k, v in value.items():
                            new_obj[key + "_" + k] = v
                new_data.append(new_obj)
            return new_data

        def flatten_json(data, parent_key='', sep='_'):
            # Flatten a nested JSON structure into a dictionary with flattened keys.
            flattened = {}
            if isinstance(data, dict):
                for key, value in data.items():
                    new_key = parent_key + sep + key if parent_key else key
                    if isinstance(value, (dict, list)):
                        flattened.update(flatten_json(value, new_key, sep=sep))
                    else:
                        flattened[new_key] = value
            elif isinstance(data, list):
                for index, item in enumerate(data):
                    new_key = parent_key + sep + str(index) if parent_key else str(index)
                    if isinstance(item, (dict, list)):
                        flattened.update(flatten_json(item, new_key, sep=sep))
                    else:
                        flattened[new_key] = item
            return flattened
        
        def flatten_dict(data, parent_key='', sep='.'):
            flattened_dict = {}
            for key, value in data.items():
                new_key = f"{parent_key}{sep}{key}" if parent_key else key
                if isinstance(value, dict):
                    flattened_dict.update(flatten_dict(value, new_key, sep=sep))
                elif isinstance(value, list):
                    for i, item in enumerate(value):
                        flattened_dict[f"{new_key}[{i}]"] = item
                else:
                    flattened_dict[new_key] = value
            return flattened_dict

        def flatten_xml(root, flattened, parent_key='', sep='_'):
            """
            Flatten an XML structure into a dictionary with flattened keys.
            """
            new_key = parent_key + sep + root.tag if parent_key else root.tag
            if root.text and root.text.strip():
                flattened[new_key] = root.text.strip()
            for child in root:
                flatten_xml(child, flattened, parent_key=new_key, sep=sep)

        def flatten_file(file_path, output_path):
            """
            Flatten the specified file and save the result to the output file.
            """
            global df
            file_extension = file_path.split('.')[-1].lower()

            if file_extension == 'json':
                # Read the JSON file as a list of objects
                # data = read_json(filename=r"C:\Users\Vijayalakshmi\Downloads\jf.json")
                with open(file_path, 'r') as f:
                    first_char = f.read(1)
                    f.seek(0)  # Reset the file pointer

                    if first_char == '[':  # JSON array
                        data = json.load(f)
                        flattened_data = []
                        for obj in data:
                            flattened_obj = flatten_dict(obj)
                            flattened_data.append(flattened_obj)

                        fieldnames = set()
                        for obj in flattened_data:
                            fieldnames.update(obj.keys())
                        # Normalize the nested JSON objects
                        #new_data = normalize_json(data=data)
                        # Create a pandas DataFrame
                        df = pd.DataFrame(flattened_data)
                    elif first_char == '{':  # JSON object
                        data = json.load(f)
                        flattened_data = flatten_dict(data)
                    else:
                        raise ValueError('Invalid JSON format')
                # df = pd.json_normalize(data)
                # flattened_data = flatten_json(data)
                # df = pd.DataFrame(flattened_data, index=[0])
            elif file_extension == 'xml':
                tree = ET.parse(file_path)
                root = tree.getroot()
                flattened_data = {}
                flatten_xml(root, flattened_data)
                df = pd.DataFrame(flattened_data, index=[0])
            elif file_extension == 'csv':
                df = pd.read_csv(file_path)
            elif file_extension == 'orc':
                with open(file_path, 'rb') as f:
                    orc_reader = ORCReader(f)
                    data = [row for row in orc_reader]
                    # flattened_data = flatten_json(data)
                    df = pd.DataFrame(data)
            elif file_extension == 'parquet':
                df = pd.read_parquet(file_path)
            elif file_extension == 'avro':
                with open(file_path, 'rb') as f:
                    avro_data = list(avro_reader(f))
                df = pd.DataFrame(avro_data)
            else:
                print("Unsupported file type.")
                return

            df.to_csv(output_path, index=False)


        # Example usage
        # file_path = r"C:\Users\Vijayalakshmi\Downloads\ecdc_cases.parquet"


        # file_path = r"C:\Users\Vijayalakshmi\Downloads\orc-file-11-format.orc"
        # file_path = r'C:\Users\Vijayalakshmi\Downloads\userdata1_orc.orc'
        # file_path = r'C:\Users\Vijayalakshmi\Datahub\pythonProject1\pythonProject1\sample.xml'
        # file_path = r'C:\Users\Vijayalakshmi\Datahub\pythonProject1\pythonProject1\sample.parquet'
        # file_path = r'C:\Users\Vijayalakshmi\Datahub\pythonProject1\pythonProject1\sampleav.avro'
        # output_path = output_path
        file_path=input_path
        output_path=output_path
        flatten_file(file_path, output_path)

        return Response({'message': 'File flattened successfully.'})
