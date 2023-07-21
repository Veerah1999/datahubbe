from urllib import request
from django.db import models
from django.contrib.auth.models import AbstractUser, UserManager
from django.contrib.auth.base_user import AbstractBaseUser
from django.db import models
from django.contrib.postgres.fields import ArrayField
import random
# from django.contrib.postgres.fields import JSONField
try:
    from django.db.models import JSONField
except ImportError:
    from django.contrib.postgres.fields import JSONField
# from django.contrib.auth.models import AbstractUser


class User(AbstractBaseUser):
   
    first_name = models.CharField(max_length=255,blank=False)
    last_name = models.CharField(max_length=255,blank=True)
    email = models.CharField(max_length=255,unique=True,blank=False)
    phone_number = models.CharField(max_length=15,blank=False)
    password = models.CharField(max_length=255,blank=False)
    alternate_phonenumber = models.CharField(max_length=15,blank=True)
    addressline_one = models.CharField(max_length=100,blank=False)
    addressline_two = models.CharField(max_length=100,blank=True)
    countryor_city = models.CharField(max_length=100,blank=False)
    postalcode = models.CharField(max_length=100,blank=False)
   # start_date = models.DateField(auto_now=True)
   # end_date = models.DateField()
    company_name = models.CharField(max_length=100,blank=True)
    company_type = models.CharField(max_length=100,blank=True)
    category = models.CharField(max_length=100,blank=False)
    username = None


    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = []
    

    class Meta:
        db_table = "register"

class tenant_user(AbstractBaseUser):
   
    email = models.CharField(max_length=255,unique=True,blank=False)
    phone_number = models.CharField(max_length=15,blank=False)
    password = models.CharField(max_length=255,blank=False)
    address= models.CharField(max_length=100,blank=False)
    country = models.CharField(max_length=100,blank=False)
    city = models.CharField(max_length=100,blank=False)
    postalcode = models.CharField(max_length=100,blank=False)
    company_name = models.CharField(max_length=100,blank=True)
    company_reg_no = models.CharField(max_length=100,blank=True)
    role = models.CharField(max_length=100,blank=True)
    company_pan_no = models.CharField(max_length=100,blank=True)
    start_date = models.DateField(auto_now=True)
    end_date = models.DateField()
    tenant_id = models.CharField(max_length=100)
    username = None


    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = []
    

    class Meta:
        db_table = "tenantregister"


class conn(models.Model):
    # connections_id=models.AutoField(auto_created=True,primary_key=True,serialize=True,verbose_name='ID')
    connection_name = models.CharField(max_length=100)
    description = models.CharField(max_length=200)
    logo_name =models.CharField(max_length=100)
    start_date = models.DateField(auto_now=True)
    end_date = models.DateField()
    is_active = models.BooleanField(default=True)
    key_param=JSONField()
    d_type=JSONField()
    def _str_(self):
        return self.connection_name

    def deactivate_expired_connections(self):
        now = date.today()
        if self.end_date < now:
            self.is_active = False
            self.save()
    class Meta:
        db_table = "conn"


class connection_detail(models.Model):

    connection_id =models.ForeignKey(conn, on_delete=models.CASCADE,related_name='connection_id')
    # connection_name = models.CharField(max_length=100)
    connection_detail = models.CharField(max_length=100)
    con_str=JSONField()
    start_date = models.DateField(auto_now=True)
    end_date = models.DateField()
    last_modified_by = models.CharField(max_length=100,null=True)
    last_modified_on = models.DateField(auto_now=True,null=True)
    created_on= models.DateField(auto_now=True,null=True)
    created_by= models.CharField(max_length=100,null=True)
    is_active = models.BooleanField(default=True)
    def _str_(self):
        return self.connection_detail

    def deactivate_expired_connections(self):
        now = date.today()
        if self.end_date < now:
            self.is_active = False
            self.save()
    class Meta:
        db_table = 'connection_detail' 

class db_config(models.Model):
    config_name = models.CharField(max_length=100)
    desc = models.CharField(max_length=100)
    source_connection_name= models.CharField(max_length=100)
    target_connection_name= models.CharField(max_length=100)
    start_date = models.DateField(auto_now=True)
    end_date = models.DateField()
    is_active = models.BooleanField(default=True) 
    Source_conn_det_id =models.ForeignKey(connection_detail, on_delete=models.CASCADE,related_name='Source_conn_det_id')
    Target_conn_det_id =models.ForeignKey(connection_detail, on_delete=models.CASCADE,related_name='Target_conn_det_id')
    
    def _str_(self):
        return self.config_name

    def deactivate_expired_connections(self):
        now = date.today()
        if self.end_date < now:
            self.is_active = False
            self.save()
    class Meta:
        db_table = 'db_config' 
        
class pipeline(models.Model):
   # Pipeline_name = models.CharField(max_length=100)
     pipeline_name= models.CharField(max_length=30, blank=True)
     email = models.EmailField(max_length=254)
     Description = models.CharField(max_length=30, blank=True)
     configuration_name = models.CharField(max_length=30,blank=True)
     Start_date = models.DateField()
     End_date = models.DateField()
     is_active = models.BooleanField(default=True) 
     config_id =models.ForeignKey(db_config, on_delete=models.CASCADE)
     def _str_(self):
        return self.pipeline_name

     def deactivate_expired_connections(self):
        now = date.today()
        if self.End_date < now:
            self.is_active = False
            self.save()
     class Meta:
        db_table = 'pipeline' 
        
class db_sql_table(models.Model):
    database_name = models.CharField(max_length=200)
    sequelize_query = models.CharField(max_length= 2000)
    sql_validation = models.CharField(max_length=200)
    sql_status = models.CharField(max_length=200) 
    start_date = models.DateField()
    end_date = models.DateField()
    is_active = models.BooleanField(default=True)

    def str(self):
        return self.db_sql_table
    def _str_(self):
        return self.database_name

    def deactivate_expired_connections(self):
        now = date.today()
        if self.end_date < now:
            self.is_active = False
            self.save()
    class Meta:
        db_table = "db_sql_extract"

class pipeline_details(models.Model):
    pipeline_detail_name = models.CharField(max_length=100)
    pipeline_dtls_desc = models.CharField(max_length=100,null=True)
    pipeline_id=models.ForeignKey(pipeline, on_delete=models.CASCADE,related_name='pipeline_id')
    sql_extract_id = models.ForeignKey(db_sql_table, on_delete=models.CASCADE,null=True,related_name='sql_extract_id')
    pipeline_name=models.CharField(max_length=100)
    sql_extract_name = models.CharField(max_length=100,null=True,blank=True)
    source_table_name= models.CharField(max_length=100)
    target_table_name = models.CharField(max_length=100)    
    start_date = models.DateField(auto_now=True)
    end_date = models.DateField()
    status = models.BooleanField(default=True)
    last_modified_by = models.CharField(max_length=100,null=True)
    last_modified_on = models.DateField(auto_now=True,null=True)
    created_on= models.DateField(auto_now=True,null=True)
    created_by= models.CharField(max_length=100,null=True)
    is_active = models.BooleanField(default=True,null=True)
    pipeline_dtls_truncate_load = models.CharField(max_length=100,null=True)
    pipeline_dtls_bench_mark_commit = models.CharField(max_length=100,null=True)
    pipeline_dtls_parallel_load_allowed = models.CharField(max_length=100,null=True)
    pipeline_dtls_parallel_thread_count = models.CharField(max_length=100,null=True)
    
    def _str_(self):
        return self.pipeline_detail_name

    def deactivate_expired_connections(self):
        now = date.today()
        if self.end_date < now:
            self.is_active = False
            self.save()
    class Meta:
        db_table = 'pipeline_details'  

class pipeline_schedule(models.Model):

    pipeline_schedule_desc = models.CharField(max_length=100)
    pipeline_schedule_name = models.CharField(max_length=100)
    pipeline_detail_name = models.CharField(max_length=100)
    pipeline_schedule_start_date = models.DateField(auto_now=True)
    pipeline_schedule_end_date = models.DateField()
    pipeline_schedule_time= models.TimeField()
    pipeline_schedule_run_imme=models.BooleanField()
    pipeline_status=models.BooleanField(default=True)
    pipeline_det_id =models.ForeignKey(pipeline_details, on_delete=models.CASCADE)

    def _str_(self):
        return self.pipeline_schedule_name

    def deactivate_expired_connections(self):
        now = date.today()
        if self.pipeline_schedule_end_date < now:
            self.pipeline_status = False
            self.save()  
    class Meta:
        db_table = "pipeline_schedule"

class ScheduleDependency(models.Model):
    # s_no = models.CharField(max_length =10)
    pipeline_schedule_dependency_name = models.CharField(max_length=50)
    parent_schedule_name =  models.CharField(max_length=50)
    child_schedule_name =  models.CharField(max_length=50)
    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)
    is_active = models.BooleanField(default=True)


    def __str__(self):
        return self.pipeline_schedule_dependency_name

    def deactivate_expired_connections(self):
        now = date.today()
        if self.end_date < now:
            self.is_active = False
            self.save()
 
    class Meta:
        db_table = 'scheduledependency'    

class role_api(models.Model):

    role_name = models.CharField(max_length=100,unique=True)
    role_desc = models.CharField(max_length=100)
    role_handling_pages = models.JSONField()
    role_start_date = models.DateField()
    role_end_date = models.DateField()
    role_status = models.BooleanField()
    
    def _str_(self):
        return self.role_name

    def deactivate_expired_connections(self):
        now = date.today()
        if self.role_end_date < now:
            self.role_status = False
            self.save()
    class Meta:
        db_table = 'role' 

class pages(models.Model):
    page_name=models.CharField(max_length=300,null=True,blank=True)
    module_name=models.CharField(max_length=300,null=True)
    page_desc=models.CharField(max_length=300,null=True,blank=True)
    start_date=models.DateField(auto_now=True)
    end_date=models.DateField(auto_now=True)
    is_active=models.BooleanField(default=True,null=True)
    created_by=models.CharField(max_length=300,null=True)
    created_on=models.CharField(max_length=300,null=True)
    
    def _str_(self):
        return self.page_name

    def deactivate_expired_connections(self):
        now = date.today()
        if self.end_date < now:
            self.is_active = False
            self.save()

    class Meta:
        db_table = 'pages' 

class role_details_api(models.Model):

    role_name = models.CharField(max_length=100)
    role_detail_name = models.CharField(max_length=100)
    role_description = models.CharField(max_length=150)
    role_handling_pages = models.JSONField()
    role_id =models.ForeignKey(role_api, on_delete=models.CASCADE,related_name='role_id')

    class Meta:
        db_table = 'role_details' 

class schema_log(models.Model):
    run_id=models.AutoField(auto_created=True,primary_key=True)
    config_id=models.IntegerField()
    schema_id=models.IntegerField()
    status=models.CharField(max_length=200)
    start_date=models.DateField()
    end_date=models.DateField(null=True)
    start_time=models.TimeField()
    end_time=models.TimeField(null=True)
    config_name = models.CharField(max_length=500)
    schema_name = models.CharField(max_length=500)
    

    class Meta:
        db_table = 'schema_log'
        
class users_role_view(models.Model):
    user_name=models.CharField(max_length=300)
    role_name=JSONField()
    start_date=models.DateField()
    end_date=models.DateField()
    is_active=models.BooleanField(default=True)
    def _str_(self):
        return self.user_name

    def deactivate_expired_connections(self):
        now = date.today()
        if self.end_date < now:
            self.is_active = False
            self.save()
    class Meta:
        db_table = 'usersrole' 

class audit_log(models.Model):
    audit_id=models.AutoField(auto_created=True,primary_key=True)
    run_id = models.IntegerField()
    schedule_id = models.IntegerField()
    start_time = models.DateField(auto_now=True)
    status = models.CharField(max_length=200)

    class Meta:
        db_table = 'audit_log'

class schedule_log(models.Model):
    run_id =models.AutoField(auto_created=True,primary_key=True)
    schedule_id = models.IntegerField()
    pipeline_id=models.IntegerField()
    level=models.CharField(max_length=300)
    start_time = models.DateField(auto_now=True)
    status = models.CharField(max_length=200)
    schedule_name = models.CharField(max_length=500)
    pipeline_name = models.CharField(max_length=500)

    class Meta:
        db_table = 'schedule_log'

class datatype(models.Model):
    config_id =models.ForeignKey(db_config, on_delete=models.CASCADE)
    config_name = models.CharField(max_length=100)
    source_id = models.IntegerField()
    source_name=models.CharField(max_length=100)  
    target_id= models.IntegerField() 
    target_name=models.CharField(max_length=100)
    datatype_mapping_name= models.CharField(max_length=100)
    datatype=JSONField()

    class Meta:
        db_table = 'dtype'

class migration_log(models.Model):
    mid_id=models.AutoField(auto_created=True,primary_key=True)
    schema_migration_id=models.IntegerField()
    source_connection_id=models.IntegerField()
    target_connection_id=models.IntegerField()
    source_schema_name=models.CharField(max_length=200)
    table_name=models.CharField(max_length=300)
    column_name=models.CharField(max_length=500)
    data_type=models.CharField(max_length=300)
    original_position=models.IntegerField()
    target_datatype=models.CharField(max_length=400,null=True)
    data_masking_column=models.CharField(max_length=300,null=True)
    row_count=models.IntegerField()

    class Meta:
        db_table = 'migration_log'

class schema_migration(models.Model):
    dec_name=models.CharField(max_length=300)
    config_id=models.ForeignKey(db_config, on_delete=models.CASCADE)
    config_name = models.CharField(max_length=100)
    source_name = models.CharField(max_length=100)
    target_name = models.CharField(max_length=100)
    schema_name=models.CharField(max_length=300,null=True)
    start_date=models.DateField(auto_now=True)
    end_date=models.DateField()
    is_active=models.BooleanField(default=True)
    all_tables=models.BooleanField()
    Play=models.BooleanField()
    target_schema_name = models.CharField(max_length=100)
    source_schema_name = models.CharField(max_length=100)
    email = models.EmailField()
    source_table_name = models.JSONField()

    class Meta:
       db_table = 'schema_migration'

class google_login(models.Model):
    access_token = models.CharField(max_length=300)
    authus = models.CharField(max_length=300)
    email = models.EmailField()
    email_verified = models.BooleanField()
    expired_on = models.IntegerField()
    family_name = models.CharField(max_length=300)
    given_name = models.CharField(max_length=300)
    locale = models.CharField(max_length=300)
    name = models.CharField(max_length=300)
    picture = models.ImageField(upload_to=None, height_field=None, width_field=None, max_length=100)
    prompt = models.CharField(max_length=300)
    scope = models.CharField(max_length=300)
    sub  = models.CharField(max_length=300)
    token_type = models.CharField(max_length=300)
    start_date = models.DateField(auto_now=True)
    end_date = models.DateField(auto_now=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = 'google_login'


class spl_columnchange(models.Model):
    config_id = models.ForeignKey(db_config, on_delete=models.CASCADE,null=True)
    pipeline_id = models.ForeignKey(pipeline,on_delete=models.CASCADE,null=True)
    column = models.CharField(max_length=300)
    replace = models.CharField(max_length=300)

    class Meta:
        db_table = 'spl_columnchange'

class teams_api_model(models.Model):
    team_name=models.CharField(max_length=300)
    role_handling_pages=ArrayField(models.JSONField())
    tenant_id = models.ForeignKey(tenant_user,on_delete=models.CASCADE,blank=True,null=True)
    class Meta:
        db_table = 'teams_api_model'

class member(models.Model):
    member_name=models.CharField(max_length=300)
    team_id=models.ForeignKey(teams_api_model, on_delete=models.CASCADE)
    tenant_id = models.ForeignKey(tenant_user,on_delete=models.CASCADE)
    mail_id=models.EmailField()

    class Meta:
      db_table = 'member'

class error_log(models.Model):
    run_id = models.IntegerField()
    schedule_id = models.IntegerField()
    start_time = models.DateField(auto_now=True)
    status = models.CharField(max_length=200)
    class Meta:
        db_table = 'error_log'



class schedule_jobs(models.Model):
    sch_job_id = models.IntegerField
    seconds =models.IntegerField()
    minutes=models.IntegerField()
    hours=models.IntegerField()
    end_date=models.DateField()
    run_imm = models.BooleanField()

    class Meta:
        db_table = 'job_schedules'




class sql_generator(models.Model):
    
    connection_name =models.ForeignKey(conn, on_delete=models.CASCADE)
    connection_detail=models.ForeignKey(connection_detail, on_delete=models.CASCADE)
    config_name =models.ForeignKey(db_config, on_delete=models.CASCADE)
    pipeline_name =models.ForeignKey(pipeline, on_delete=models.CASCADE)
    pipeline_detail_name  =models.ForeignKey(pipeline_details, on_delete=models.CASCADE)
    pipeline_schedule_name =models.ForeignKey(pipeline_schedule, on_delete=models.CASCADE)
    pipeline_schedule_dependency_name =models.ForeignKey(ScheduleDependency, on_delete=models.CASCADE)
    role_name=models.ForeignKey(role_api, on_delete=models.CASCADE)
    role_detail_name=models.ForeignKey(role_details_api, on_delete=models.CASCADE)
    user_name=models.ForeignKey(users_role_view, on_delete=models.CASCADE)
    page_name=models.ForeignKey(pages, on_delete=models.CASCADE)
    database_name =models.ForeignKey(db_sql_table, on_delete=models.CASCADE)
    columns=models.JSONField()
    datatype=models.JSONField()
    
    class Meta:
        db_table = "sql_generator"

class preaduit(models.Model):
   
    id= models.AutoField(auto_created=True,primary_key=True)
    database_name= models.CharField(max_length=15,blank=False)

    class Meta:
        db_table = "preaduit"
        
class user_api(models.Model):
   
    first_name = models.CharField(max_length=255)
    last_name = models.CharField(max_length=255,null=True)
    email = models.CharField(max_length=255,unique=True)
    employee_id= models.CharField(max_length=200)
    password = models.CharField(max_length=255,blank=False)
    start_date = models.DateField(auto_now=True)
    end_date = models.DateField()
    is_active = models.BooleanField(default=True)
    
    

    class Meta:
        db_table = "user_api"


class price_tables(models.Model):
    region_name=models.CharField(max_length=300)
    ami=models.CharField(max_length=300)
    Architecture=models.CharField(max_length=300)
    Description=models.CharField(max_length=300)
    Instance_Type=models.CharField(max_length=300)
    Family=models.CharField(max_length=300)
    Memory=models.CharField(max_length=300)
    V_CPU=models.IntegerField()
    Storage=models.IntegerField()
    Price=models.FloatField()

    class Meta:
        db_table = "price_calculators"


class f_pass(models.Model):
    email = models.CharField(max_length=200)
    otp=models.CharField( max_length=50)

    class Meta:
        db_table = "f_pass"

class spl_columnchange(models.Model):
    config_id = models.ForeignKey(db_config, on_delete=models.CASCADE,null=True)
    pipeline_id = models.ForeignKey(pipeline,on_delete=models.CASCADE,null=True)
    table_name = models.CharField(max_length=200)
    column = models.CharField(max_length=300)
    replace = models.CharField(max_length=300)
    start_date = models.DateField(auto_now=True)
    end_date = models.DateField()

    class Meta:
        db_table = 'spl_columnchange'

class schema_error_log(models.Model):
    run_id = models.IntegerField()
    schedule_id = models.IntegerField()
    start_time = models.DateField(auto_now=True)
    status = models.CharField(max_length=200)
    class Meta:
        db_table = 'schema_error_log'

class schema_audit_log(models.Model):
    audit_id=models.AutoField(auto_created=True,primary_key=True)
    run_id = models.IntegerField()
    schedule_id = models.IntegerField()
    start_time = models.DateField(auto_now=True)
    status = models.CharField(max_length=200)

    class Meta:
        db_table = 'schema_audit_log'

class column_name(models.Model):
    tenant_id = models.ForeignKey(tenant_user, on_delete=models.CASCADE, null=True)
    table_name=models.CharField(max_length=100)
    column_name=JSONField()
    start_date=models.DateField()
    end_date=models.DateField()
    is_active = models.BooleanField(default=True)
    config_id = models.ForeignKey(db_config, on_delete=models.CASCADE)

    class Meta:
        db_table = 'column_name'

class column_config(models.Model):
    tenant_id = models.ForeignKey(tenant_user, on_delete=models.CASCADE, null=True)
    table_name=models.CharField(max_length=100)
    column_desc=JSONField()
    start_date=models.DateField()
    end_date=models.DateField()
    is_active = models.BooleanField(default=True)
    col_config_id = models.ForeignKey(column_name, on_delete=models.CASCADE)

    class Meta:
        db_table = 'column_config'

class d_transform(models.Model):
    dec_name=models.CharField(max_length=300)
    config_id=models.ForeignKey(db_config, on_delete=models.CASCADE)
    transform_name = models.CharField(max_length=100)
    schema_name = models.CharField(max_length=100)
    config_name = models.CharField(max_length=100)
    source_name = models.CharField(max_length=100)
    target_name = models.CharField(max_length=100)
    start_date=models.DateField(auto_now=True)
    end_date=models.DateField()
    is_active=models.BooleanField(default=True)
    all_tables=models.BooleanField()
   # target_transform_name = models.CharField(max_length=100)
    source_transform_name = models.CharField(max_length=100)
    source_table_name = models.JSONField()
    # config_id = models.ForeignKey(db_config, on_delete=models.CASCADE, null=True)
    # transform_name = models.CharField(max_length=100)
    # config_name = models.CharField(max_length=200)
    # target_table = models.CharField(max_length=100)
    # start_date = models.DateField(auto_now=True)
    # end_date = models.DateField()
    # is_active = models.BooleanField(default=True)


    class Meta:
        db_table = 'd_transform'

