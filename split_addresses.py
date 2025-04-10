import os
import json
from collections import defaultdict
import mmap

def process_addresses():
    # 确保data目录存在
    if not os.path.exists('data'):
        os.makedirs('data')
    
    # 创建字典来存储不同前缀的地址
    address_dict = defaultdict(list)
    
    # 使用内存映射文件来高效读取大文件
    with open('ethereum_addresses.txt', 'r') as f:
        # 使用内存映射
        mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        
        # 批量处理，每批10000个地址
        batch_size = 10000
        current_batch = []
        
        for line in iter(mm.readline, b''):
            try:
                address = line.decode('utf-8').strip()
                if not address:
                    continue
                # 去掉0x前缀
                if address.startswith('0x'):
                    address = address[2:]
                prefix = address[:2].lower()
                current_batch.append((prefix, address))
                
                # 当批次达到指定大小时处理
                if len(current_batch) >= batch_size:
                    for prefix, addr in current_batch:
                        address_dict[prefix].append(addr)
                    current_batch = []
                    
                    # 定期写入文件并清空内存
                    for prefix, addresses in address_dict.items():
                        output_file = os.path.join('data', f'{prefix}.json')
                        # 如果文件已存在，先读取现有数据
                        existing_data = []
                        if os.path.exists(output_file):
                            with open(output_file, 'r') as f:
                                try:
                                    existing_data = json.load(f)
                                except json.JSONDecodeError:
                                    existing_data = []
                        
                        # 合并现有数据和新数据
                        all_addresses = existing_data + addresses
                        
                        # 写入更新后的数据
                        with open(output_file, 'w') as f:
                            json.dump(all_addresses, f, indent=2)
                        
                        print(f'已处理前缀 {prefix}，当前批次 {len(addresses)} 个地址，总计 {len(all_addresses)} 个地址')
                    address_dict.clear()
                    
            except Exception as e:
                print(f"处理行时出错: {e}")
                continue
        
        # 处理剩余的批次
        if current_batch:
            for prefix, addr in current_batch:
                address_dict[prefix].append(addr)
        
        # 处理最后一批数据
        for prefix, addresses in address_dict.items():
            output_file = os.path.join('data', f'{prefix}.json')
            # 如果文件已存在，先读取现有数据
            existing_data = []
            if os.path.exists(output_file):
                with open(output_file, 'r') as f:
                    try:
                        existing_data = json.load(f)
                    except json.JSONDecodeError:
                        existing_data = []
            
            # 合并现有数据和新数据
            all_addresses = existing_data + addresses
            
            # 写入更新后的数据
            with open(output_file, 'w') as f:
                json.dump(all_addresses, f, indent=2)
            
            print(f'最后处理前缀 {prefix}，共 {len(addresses)} 个地址，总计 {len(all_addresses)} 个地址')
        
        mm.close()

if __name__ == '__main__':
    process_addresses() 