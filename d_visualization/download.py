from django.http import HttpResponse
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying, flatten_dict_column
import PyPDF2
import io
import os 
from wsgiref.util import FileWrapper

from django.conf import settings


class TableauConnector:
    def __init__(self, config):
        self.config = config
        self.conn = None

    def connect_to_tableau_server(self):
        # Establish a connection to Tableau Server
        self.conn = TableauServerConnection(self.config, env='tableau_online')
        self.conn.sign_in()
        print("Signed in Successfully")

    def get_workbook_views(self, workbook_name):
        # Retrieve the views associated with a given workbook
        views_df = querying.get_views_dataframe(self.conn)
        views_df = flatten_dict_column(views_df, keys=["name", "id"], col_name="workbook")
        relevant_views_df = views_df[views_df["workbook_name"] == workbook_name]
        return relevant_views_df['id'].tolist()

    def download_workbook_as_pdf(self, workbook_name):
        # Set parameters for PDF export
        pdf_params = {
            "pdf_orientation": "orientation=Landscape",
            "pdf_layout": "type=A4"
        }
        pdf_writer = PyPDF2.PdfWriter()

        view_ids = self.get_workbook_views(workbook_name)

        # Iterate through the view IDs
        for view_id in view_ids:
            # Query the PDF content for each view
            view_pdf = self.conn.query_view_pdf(view_id=view_id, parameter_dict=pdf_params)
            pdf_content = io.BytesIO(view_pdf.content)
            pdf_reader = PyPDF2.PdfReader(pdf_content)

            # Append each page of the PDF to the writer
            for page_number in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_number]
                pdf_writer.add_page(page)

        # Save the combined PDF file
        with open(rf'D:\Tableau_downloader\pdf\media\Combined_views.pdf', 'wb') as file:
            pdf_writer.write(file)
        print("Tableau workbook as a PDF file is downloaded successfully")
#Tableau_downloader\pdf\media\

class Downloader(APIView):
    def post(self, request):
        # Get the server, API version, personal access token name, and workbook name from the request data
        server = request.data.get('server')
        api_version = request.data.get('api_version')
        personal_access_token_name = request.data.get('personal_access_token_name')
        personal_access_token_secret = request.data.get('personal_access_token_secret')
        site_name = request.data.get('site_name')
        site_url = request.data.get('site_url')
        workbook_name = request.data.get('workbook_name')

        # Configuration details for Tableau Server
        config = {
            'tableau_online': {
                'server': server,
                'api_version': api_version,
                'personal_access_token_name': personal_access_token_name,
                'personal_access_token_secret': personal_access_token_secret,
                'site_name': site_name,
                'site_url': site_url
            }
        }

        try:
            # Instantiate the TableauConnector class
            tableau_connector = TableauConnector(config)

            # Connect to Tableau Server
            tableau_connector.connect_to_tableau_server()

            # Download the specified workbook as a PDF
            tableau_connector.download_workbook_as_pdf(workbook_name)

            return Response({'message': 'Tableau workbook as a PDF file is downloaded successfully'}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({'message': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
# class Get(APIView):
#     def get(self, request, *args, **kwargs):
#                 short_report = open("D:\combined_views.pdf", 'rb')
#                 response = HttpResponse(FileWrapper(short_report), content_type='application/pdf')
#                 return Response({'detail': 'this works',
#                     'report': response})

