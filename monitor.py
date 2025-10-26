#!/usr/bin/env python3
import boto3
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

def load_config():
    """从config.json加载配置"""
    config_path = Path(__file__).parent / 'config.json'
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"错误: 配置文件 {config_path} 不存在")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"错误: 配置文件格式错误 - {e}")
        sys.exit(1)

def check_backup_status():
    """检查备份状态"""
    config = load_config()
    backup_config = config['backup_config']
    
    tables = backup_config['tables']
    bucket_name = backup_config['s3_bucket']
    source_region = backup_config['source_region']
    target_region = backup_config['target_region']
    
    dynamodb = boto3.client('dynamodb', region_name=source_region)
    s3 = boto3.client('s3', region_name=target_region)
    
    print("=== DynamoDB备份状态检查 ===")
    print(f"检查时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"表数量: {len(tables)}")
    print(f"S3存储桶: {bucket_name} ({target_region})")
    
    for table_info in tables:
        table_arn = table_info['table_arn']
        table_name = table_info['table_name']
        
        print(f"\n--- 表: {table_name} ---")
        
        # 检查最近的导出任务
        try:
            exports = dynamodb.list_exports(TableArn=table_arn, MaxResults=5)
            
            print(f"最近5次导出任务:")
            for export in exports['ExportSummaries']:
                export_time = export['ExportTime'].strftime('%Y-%m-%d %H:%M:%S')
                status = export['ExportStatus']
                print(f"  {export_time} | {status}")
                
        except Exception as e:
            print(f"获取导出列表失败: {e}")
        
        # 检查S3备份文件
        print(f"S3备份文件检查:")
        try:
            # 检查今天的备份
            today = datetime.now().strftime('%Y/%m/%d')
            daily_prefix = f"daily/{today}/{table_name}/"
            
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=daily_prefix)
            if 'Contents' in response:
                total_size = sum(obj['Size'] for obj in response['Contents'])
                print(f"  今日备份: {len(response['Contents'])} 文件, {total_size/1024/1024:.2f} MB")
            else:
                print(f"  今日备份: 未找到")
            
            # 检查最近7天的备份
            print(f"  最近7天备份:")
            for i in range(7):
                date = (datetime.now() - timedelta(days=i)).strftime('%Y/%m/%d')
                prefix = f"daily/{date}/{table_name}/"
                response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
                if 'Contents' in response:
                    total_size = sum(obj['Size'] for obj in response['Contents'])
                    print(f"    {date}: {total_size/1024/1024:.2f} MB")
                
        except Exception as e:
            print(f"检查S3文件失败: {e}")

def manual_backup():
    """手动触发备份"""
    config = load_config()
    source_region = config['backup_config']['source_region']
    
    lambda_client = boto3.client('lambda', region_name=source_region)
    
    try:
        response = lambda_client.invoke(
            FunctionName='DynamoDBBackupFunction',
            InvocationType='RequestResponse',
            Payload=json.dumps({})
        )
        
        result = json.loads(response['Payload'].read())
        print(f"手动备份触发成功: {result}")
        
    except Exception as e:
        print(f"手动备份失败: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == 'backup':
            manual_backup()
        else:
            print("用法: python monitor.py [backup]")
    else:
        check_backup_status()
