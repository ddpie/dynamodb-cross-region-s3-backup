#!/usr/bin/env python3
import boto3
import json
import sys
import time
from datetime import datetime
from pathlib import Path

def load_table_schema(s3_client=None, bucket=None, date_prefix=None):
    """从table_schema.json或S3备份中加载表结构"""
    schema_path = Path(__file__).parent / 'table_schema.json'
    
    # 优先从S3备份中读取
    if s3_client and bucket and date_prefix:
        try:
            schema_key = f"{date_prefix}table_schema.json"
            response = s3_client.get_object(Bucket=bucket, Key=schema_key)
            schema = json.loads(response['Body'].read())
            print(f"从S3备份加载表结构: {schema_key}")
            return schema
        except Exception as e:
            print(f"从S3加载表结构失败: {e}，尝试本地文件")
    
    # 回退到本地文件
    try:
        with open(schema_path, 'r') as f:
            schema = json.load(f)
            print(f"从本地文件加载表结构: {schema_path}")
            return schema
    except FileNotFoundError:
        print(f"错误: 表结构文件不存在")
        print("请先运行: python export_table_schema.py YOUR_TABLE us-east-1")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"错误: 表结构文件格式错误 - {e}")
        sys.exit(1)

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

def restore_from_s3(table_name, target_region='us-west-2', restore_date=None):
    """从S3备份恢复DynamoDB表"""
    
    config = load_config()
    backup_config = config['backup_config']
    
    s3_bucket = backup_config['s3_bucket']
    s3_region = backup_config['target_region']
    new_table_name = f"{table_name}-restored-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    
    dynamodb = boto3.client('dynamodb', region_name=target_region)
    s3 = boto3.client('s3', region_name=s3_region)
    
    print(f"=== DynamoDB数据恢复 ===")
    print(f"原表名: {table_name}")
    print(f"新表名: {new_table_name}")
    print(f"目标区域: {target_region}")
    print(f"S3存储桶: {s3_bucket} ({s3_region})")
    
    try:
        if restore_date:
            # 查找指定日期的导出数据
            date_prefix = f"daily/{restore_date}/{table_name}/"
            response = s3.list_objects_v2(
                Bucket=s3_bucket,
                Prefix=f"{date_prefix}AWSDynamoDB/",
                Delimiter='/'
            )
            if 'CommonPrefixes' not in response:
                raise Exception(f"未找到备份: {date_prefix}")
            export_id_prefix = response['CommonPrefixes'][0]['Prefix']
            s3_prefix = f"{export_id_prefix}data/"
        else:
            # 使用最新的备份
            s3_prefix = get_latest_backup_prefix(s3, s3_bucket, table_name)
            date_prefix = s3_prefix.split('AWSDynamoDB/')[0]
        
        print(f"使用备份: {s3_prefix}")
        
        # 加载表结构（优先从S3备份）
        table_schema = load_table_schema(s3, s3_bucket, date_prefix)
        
        # 构建表创建参数
        table_params = table_schema.copy()
        table_params['TableName'] = new_table_name
        
        # 导入表
        response = dynamodb.import_table(
            S3BucketSource={
                'S3Bucket': s3_bucket,
                'S3KeyPrefix': s3_prefix
            },
            InputFormat='DYNAMODB_JSON',
            TableCreationParameters=table_params
        )
        
        print(f"\n导入任务已启动:")
        print(f"导入ARN: {response['ImportTableDescription']['ImportArn']}")
        print(f"状态: {response['ImportTableDescription']['ImportStatus']}")
        
        # 监控导入进度
        monitor_import_progress(dynamodb, response['ImportTableDescription']['ImportArn'])
        
    except Exception as e:
        print(f"\n恢复失败: {e}")
        print("\n提示：")
        print("1. 检查table_schema.json是否存在且格式正确")
        print("2. 使用以下命令重新导出表结构：")
        print(f"   python export_table_schema.py {table_name} {backup_config['source_region']}")

def get_latest_backup_prefix(s3, bucket, table_name):
    """获取最新备份的S3前缀（包含AWSDynamoDB/<EXPORT-ID>/data/）"""
    try:
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix="daily/",
            Delimiter='/'
        )
        
        if 'CommonPrefixes' not in response:
            raise Exception("未找到备份")
        
        # 获取最新的日期目录
        date_prefixes = [prefix['Prefix'] for prefix in response['CommonPrefixes']]
        latest_date_prefix = sorted(date_prefixes)[-1]
        
        # 查找表的AWSDynamoDB子目录
        table_prefix = f"{latest_date_prefix}{table_name}/"
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=f"{table_prefix}AWSDynamoDB/",
            Delimiter='/'
        )
        
        if 'CommonPrefixes' not in response:
            raise Exception(f"未找到导出数据: {table_prefix}AWSDynamoDB/")
        
        # 获取导出ID目录
        export_id_prefix = response['CommonPrefixes'][0]['Prefix']
        data_prefix = f"{export_id_prefix}data/"
        
        return data_prefix
        
    except Exception as e:
        raise Exception(f"获取最新备份失败: {e}")

def monitor_import_progress(dynamodb, import_arn):
    """监控导入进度"""
    print("\n监控导入进度...")
    while True:
        try:
            response = dynamodb.describe_import(ImportArn=import_arn)
            status = response['ImportTableDescription']['ImportStatus']
            
            if status == 'COMPLETED':
                print("导入完成!")
                print(f"处理的项目数: {response['ImportTableDescription'].get('ProcessedItemCount', 'N/A')}")
                print(f"导入的项目数: {response['ImportTableDescription'].get('ImportedItemCount', 'N/A')}")
                break
            elif status == 'FAILED':
                print("导入失败!")
                print(f"失败原因: {response['ImportTableDescription'].get('FailureMessage', 'Unknown')}")
                break
            else:
                print(f"导入中... 状态: {status}")
                time.sleep(30)
                
        except Exception as e:
            print(f"监控失败: {e}")
            break

def list_available_backups(table_name=None):
    """列出可用的备份"""
    config = load_config()
    backup_config = config['backup_config']
    
    s3_bucket = backup_config['s3_bucket']
    s3_region = backup_config['target_region']
    tables = backup_config['tables']
    
    s3 = boto3.client('s3', region_name=s3_region)
    
    print("=== 可用备份列表 ===")
    print(f"S3存储桶: {s3_bucket} ({s3_region})")
    
    # 如果指定表名，只列出该表的备份
    if table_name:
        table_list = [{'table_name': table_name}]
    else:
        table_list = tables
    
    for table_info in table_list:
        tbl_name = table_info['table_name']
        print(f"\n--- 表: {tbl_name} ---")
        print("最近30天备份:")
        
        try:
            response = s3.list_objects_v2(Bucket=s3_bucket, Prefix='daily/', Delimiter='/')
            if 'CommonPrefixes' in response:
                date_prefixes = sorted([p['Prefix'] for p in response['CommonPrefixes']], reverse=True)[:30]
                for date_prefix in date_prefixes:
                    backup_date = date_prefix.replace('daily/', '').rstrip('/')
                    # 检查该日期是否有该表的备份
                    table_prefix = f"{date_prefix}{tbl_name}/"
                    check = s3.list_objects_v2(Bucket=s3_bucket, Prefix=table_prefix, MaxKeys=1)
                    if 'Contents' in check:
                        print(f"  {backup_date}")
            else:
                print("  未找到备份")
        except Exception as e:
            print(f"  获取备份列表失败: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法:")
        print("  python restore.py list [table_name]                    # 列出可用备份")
        print("  python restore.py restore TABLE_NAME [region] [date]   # 恢复数据")
        print("")
        print("示例:")
        print("  python restore.py list")
        print("  python restore.py list MyTable")
        print("  python restore.py restore MyTable us-west-2")
        print("  python restore.py restore MyTable us-west-2 2025/10/26")
        print("")
        print("重要提示:")
        print("  在首次使用前，必须先导出表结构：")
        print("  python export_table_schema.py YOUR_TABLE us-east-1")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == 'list':
        table_name = sys.argv[2] if len(sys.argv) > 2 else None
        list_available_backups(table_name)
    elif command == 'restore':
        if len(sys.argv) < 3:
            print("错误: restore命令需要指定表名")
            print("用法: python restore.py restore TABLE_NAME [region] [date]")
            sys.exit(1)
        table_name = sys.argv[2]
        region = sys.argv[3] if len(sys.argv) > 3 else 'us-west-2'
        date = sys.argv[4] if len(sys.argv) > 4 else None
        restore_from_s3(table_name, region, date)
    else:
        print(f"未知命令: {command}")
