# Generated by Django 4.1.3 on 2023-07-11 05:36

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('datahub_v3_app', '0005_d_transform_schema_name'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='d_transform',
            name='target_transform_name',
        ),
    ]
