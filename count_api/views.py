from datahub_v3_app.models import *
from rest_framework.views import APIView
from rest_framework.response import Response

class Count_View(APIView):


    def get(self, request, pk=None, ):
          if pk:
                data = self.get_object(pk)
               # serializer = count_serializer(data)

          else:

                var_connections = {}
                var_pipelines = {}
                var_pipeline_sch = {}
                var_db_configs = {}
                var_sql_extract = {}
                var_schedule_dependency ={}
                var_pipeline_detail ={}
                var_connection_details ={}
                var_role_apis ={}








                # data =role_api.objects.all()
                # serializer = count_serializer(data, many=True)

                var_message_count1 = pipeline_schedule.objects.filter(pipeline_status='True').count()
                var_message_count2 = pipeline.objects.filter(is_active='True').count()
                var_message_count3 = conn.objects.filter(is_active='True').count()
                var_message_count4 = db_config.objects.filter(is_active='True').count()
                var_message_count5 = pipeline_schedule.objects.filter(pipeline_status='False').count()
                var_message_count6 = pipeline.objects.filter(is_active='False').count()
                var_message_count7 = conn.objects.filter(is_active='False').count()
                var_message_count8 = db_config.objects.filter(is_active='False').count()
                var_message_count9 = db_sql_table.objects.filter(is_active='True').count()
                var_message_count10 = ScheduleDependency.objects.filter(is_active='True').count()
                var_message_count11 = pipeline_details.objects.filter(is_active='True').count()
                var_message_count12 = connection_detail.objects.filter(is_active='True').count()
                var_message_count13= db_sql_table.objects.filter(is_active='False').count()
                var_message_count14 = ScheduleDependency.objects.filter(is_active='False').count()
                var_message_count15 = pipeline_details.objects.filter(is_active='False').count()
                var_message_count16 = connection_detail.objects.filter(is_active='False').count()
                var_message_count17 = role_api.objects.filter(role_status='True').count()
                var_message_count18 = role_api.objects.filter(role_status='False').count()
               # message_count20 =print("connection")


                var_dataset = []

                var_connections.update({"active":var_message_count3})
                var_connections.update({"inactive": var_message_count7})
                var_connections.update({"total": var_message_count7+var_message_count3})
                var_connections.update({"name": "connection"})
                var_dataset.append(var_connections)

                var_pipelines.update({"active":var_message_count2})
                var_pipelines.update(({"inactive":var_message_count6}))
                var_pipelines.update(({"total": var_message_count6+var_message_count2}))
                var_pipelines.update({"name":"pipeline"})
                var_dataset.append(var_pipelines)

                var_pipeline_sch.update({"active":var_message_count1})
                var_pipeline_sch.update({"inactive": var_message_count5})
                var_pipeline_sch.update({"total": var_message_count5+var_message_count1})
                var_pipeline_sch.update({"name":"pipeline schedule"})
                var_dataset.append((var_pipeline_sch))


                var_db_configs.update({"active":var_message_count4})
                var_db_configs.update({"inactive": var_message_count8})
                var_db_configs.update({"total": var_message_count8+var_message_count4})
                var_db_configs.update({"name":"db configurations"})
                var_dataset.append((var_db_configs))

                var_sql_extract.update({"active":var_message_count9})
                var_sql_extract.update({"inactive":var_message_count13})
                var_sql_extract.update({"total": var_message_count13+var_message_count9})
                var_sql_extract.update({"name":"sql extract"})
                var_dataset.append(var_sql_extract)

                var_schedule_dependency.update({"active":var_message_count10})
                var_schedule_dependency.update({"inactive":var_message_count14})
                var_schedule_dependency.update({"total": var_message_count14+var_message_count10})
                var_schedule_dependency.update({"name":"schedule dependency"})
                var_dataset.append(var_schedule_dependency)

                var_pipeline_detail.update({"active":var_message_count11})
                var_pipeline_detail.update({"inactive":var_message_count15})
                var_pipeline_detail.update({"total": var_message_count15+var_message_count11})
                var_pipeline_detail.update({"name":"pipeline_detail"})
                var_dataset.append(var_pipeline_detail)

                var_connection_details.update({"active":var_message_count12})
                var_connection_details.update({"inactive":var_message_count16})
                var_connection_details.update({"total":var_message_count16+var_message_count12})
                var_connection_details.update({"name":"connection details"})
                var_dataset.append(var_connection_details)

                var_role_apis.update({"active":var_message_count17})
                var_role_apis.update({"inactive":var_message_count18})
                var_role_apis.update({"total":var_message_count17+var_message_count18})
                var_role_apis.update({"name":"role api"})
                var_dataset.append(var_role_apis)

                return Response(var_dataset)