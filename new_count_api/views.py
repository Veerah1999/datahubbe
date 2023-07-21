from datahub_v3_app.models import *
from rest_framework.views import APIView
from rest_framework.response import Response

class NewCount_View(APIView):


    def get(self, request, pk=None, ):
          if pk:
                data = self.get_object(pk)

          else:

                var_connections = {}
                var_pipelines = {}
                var_pipeline_schedules = {}
                var_db_configs = {}
                var_connection1 = {}
                var_pipeline2 = {}
                var_pipeline_schedule3 = {}
                var_db_configs4 = {}


                var_message_count1 = pipeline_schedule.objects.filter(pipeline_status='True').count()
                var_message_count2 = pipeline.objects.filter(is_active='True').count()
                var_message_count3 = conn.objects.filter(is_active='True').count()
                var_message_count4 = db_config.objects.filter(is_active='True').count()
                var_message_count5 = pipeline_schedule.objects.filter(pipeline_status='False').count()
                var_message_count6 = pipeline.objects.filter(is_active='False').count()
                var_message_count7 = conn.objects.filter(is_active='False').count()
                var_message_count8 = db_config.objects.filter(is_active='False').count()

                var_dataset = []

                var_connections.update({"Name": "Active"})
                var_connections.update({"value": "Connection"})
                var_connections.update({"Count":var_message_count3})
                var_dataset.append(var_connections)

                var_db_configs.update({"Name": "Active"})
                var_db_configs.update({"value": "Config"})
                var_db_configs.update({"Count": var_message_count4})
                var_dataset.append((var_db_configs))

                var_pipelines.update({"Name":"Active"})
                var_pipelines.update({"value":"Pipeline"})
                var_pipelines.update({"Count": var_message_count2})
                var_dataset.append(var_pipelines)

                var_pipeline_schedules.update({"Name":"Active"})
                var_pipeline_schedules.update({"value":"Schedules"})
                var_pipeline_schedules.update({"Count": var_message_count1})
                var_dataset.append((var_pipeline_schedules))

                var_connection1.update({"Name":"Inactive"})
                var_connection1.update({"value": "Connection"})
                var_connection1.update({"Count": var_message_count7})
                var_dataset.append(var_connection1)

                var_db_configs4.update({"Name": "Inactive"})
                var_db_configs4.update({"value": "Config"})
                var_db_configs4.update({"Count": var_message_count8})
                var_dataset.append((var_db_configs4))

                var_pipeline2.update(({"Name":"Inactive"}))
                var_pipeline2.update({"value":"Pipeline"})
                var_pipeline2.update(({"Count": var_message_count6}))
                var_dataset.append(var_pipeline2)

                var_pipeline_schedule3.update({"Name":"Inactive"})
                var_pipeline_schedule3.update({"value": "Schedules"})
                var_pipeline_schedule3.update({"Count": var_message_count5})
                var_dataset.append((var_pipeline_schedule3))

                return Response(var_dataset)


class NewCount_Detail_View(APIView):

    def get(self, request, pk=None, ):
          if pk:
                data = self.get_object(pk)

          else:

                var_sql_extract = {}
                var_schedule_dependency = {}
                var_pipeline_detail = {}
                var_connection_details = {}
                var_role_apis = {}
                var_sql_extract1 = {}
                var_schedule_dependency2 = {}
                var_pipeline_detail3 = {}
                var_connection_detail4 = {}
                var_role_apis5 = {}

                var_message_count9  = db_sql_table.objects.filter(is_active='True').count()
                var_message_count10 = ScheduleDependency.objects.filter(is_active='True').count()
                var_message_count11 = pipeline_details.objects.filter(is_active='True').count()
                var_message_count12 = connection_detail.objects.filter(is_active='True').count()
                var_message_count13 = db_sql_table.objects.filter(is_active='False').count()
                var_message_count14 = ScheduleDependency.objects.filter(is_active='False').count()
                var_message_count15 = pipeline_details.objects.filter(is_active='False').count()
                var_message_count16 = connection_detail.objects.filter(is_active='False').count()
                var_message_count17 = role_api.objects.filter(role_status='True').count()
                var_message_count18 = role_api.objects.filter(role_status='False').count()

                var_dataset = []

                var_connection_details.update({"name": "Active"})
                var_connection_details.update({"value": "ConnectionDetail"})
                var_connection_details.update({"count": var_message_count12})
                var_dataset.append(var_connection_details)

                var_sql_extract.update({"name":"Active"})
                var_sql_extract.update({"value":"ExtractSQL"})
                var_sql_extract.update({"count": var_message_count9})
                var_dataset.append(var_sql_extract)

                var_pipeline_detail.update({"name": "Active"})
                var_pipeline_detail.update({"value": "PipelineDetail"})
                var_pipeline_detail.update({"count": var_message_count11})
                var_dataset.append(var_pipeline_detail)

                var_role_apis.update({"name": "Active"})
                var_role_apis.update({"value": "Role"})
                var_role_apis.update({"count": var_message_count17})
                var_dataset.append(var_role_apis)

                var_schedule_dependency.update({"name":"Active"})
                var_schedule_dependency.update({"value":"ScheduleDepend"})
                var_schedule_dependency.update({"count": var_message_count10})
                var_dataset.append(var_schedule_dependency)

                var_connection_detail4.update({"name": "Inactive"})
                var_connection_detail4.update({"value": "ConnectionDetail"})
                var_connection_detail4.update({"count": var_message_count16})
                var_dataset.append(var_connection_detail4)

                var_sql_extract1.update({"name": "Inactive"})
                var_sql_extract1.update({"value": "ExtractSQL"})
                var_sql_extract1.update({"count": var_message_count13})
                var_dataset.append(var_sql_extract1)

                var_pipeline_detail3.update({"name": "Inactive"})
                var_pipeline_detail3.update({"value": "PipelineDetail"})
                var_pipeline_detail3.update({"count": var_message_count15})
                var_dataset.append(var_pipeline_detail3)

                var_role_apis5.update({"name": "Inactive"})
                var_role_apis5.update({"value": "Role"})
                var_role_apis5.update({"count": var_message_count18})
                var_dataset.append(var_role_apis5)

                var_schedule_dependency2.update({"name": "Inactive"})
                var_schedule_dependency2.update({"value": "ScheduleDepend"})
                var_schedule_dependency2.update({"count": var_message_count14})
                var_dataset.append(var_schedule_dependency2)

                return Response(var_dataset)
          
class TotalCount_View(APIView):


    def get(self, request, pk=None, ):
          if pk:
                data = self.get_object(pk)

          else:

                var_connections = {}
                var_pipelines = {}
                var_pipeline_schedules = {}
                var_db_configs = {}
                
                var_message_count1 = pipeline_schedule.objects.filter(pipeline_status='True').count()
                var_message_count2 = pipeline.objects.filter(is_active='True').count()
                var_message_count3 = conn.objects.filter(is_active='True').count()
                var_message_count4 = db_config.objects.filter(is_active='True').count()
                var_message_count5 = pipeline_schedule.objects.filter(pipeline_status='False').count()
                var_message_count6 = pipeline.objects.filter(is_active='False').count()
                var_message_count7 = conn.objects.filter(is_active='False').count()
                var_message_count8 = db_config.objects.filter(is_active='False').count()
            
                var_message_count9 =  var_message_count3+var_message_count7
                var_message_count10 = var_message_count4 + var_message_count8
                var_message_count11 = var_message_count2+var_message_count6
                var_message_count12 = var_message_count1+var_message_count5

                var_dataset = []
                var_connections.update({"name": "Connections"})
                var_connections.update({"totalcount": var_message_count3+var_message_count7})
                var_connections.update({"overallpercent":round( var_message_count3/var_message_count9*100)})
                var_dataset.append(var_connections)

                var_db_configs.update({"name": "Configurations"})
                var_db_configs.update({"totalcount": var_message_count4+var_message_count8})
                var_db_configs.update({"overallpercent": round(var_message_count4/var_message_count10*100)})
                var_dataset.append((var_db_configs))

                var_pipelines.update({"name":"Pipelines"})
                var_pipelines.update({"totalcount": var_message_count2+var_message_count6})
                var_pipelines.update({"overallpercent":round( var_message_count2/var_message_count11*100)})
                var_dataset.append(var_pipelines)

                var_pipeline_schedules.update({"name":"Schedules"})
                var_pipeline_schedules.update({"totalcount": var_message_count1+var_message_count5})
                var_pipeline_schedules.update({"overallpercent": round(var_message_count1/var_message_count12*100)})
                var_dataset.append((var_pipeline_schedules))

                return Response(var_dataset)
