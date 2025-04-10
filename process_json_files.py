import os
import json
import csv
from web3 import Web3
import threading
from queue import Queue
import time

# 连接到以太坊主网
eth = "http://192.168.31.100:8547"  # Alchemy免费节点
web3 = Web3(Web3.HTTPProvider(eth))

# WETH合约地址和ABI
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
WETH_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "","type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "","type": "uint256"}],
        "type": "function"
    }
]

def get_eth_balance(address):
    # 获取地址的ETH余额
    address = web3.to_checksum_address(address)
    balance = web3.eth.get_balance(address)
    # 将Wei转换为ETH
    return web3.from_wei(balance, 'ether')

def get_weth_balance(address):
    # 获取地址的WETH余额
    weth_contract = web3.eth.contract(address=WETH_ADDRESS, abi=WETH_ABI)
    address = web3.to_checksum_address(address)
    balance = weth_contract.functions.balanceOf(address).call()
    return web3.from_wei(balance, 'ether')

def worker(address_queue, result_queue):
    while True:
        try:
            address = address_queue.get_nowait()
        except:
            break
            
        try:
            eth_balance = get_eth_balance(address)
            weth_balance = get_weth_balance(address)
            result_queue.put((address, eth_balance, weth_balance))
            print(f"地址: {address}, ETH余额: {eth_balance}, WETH余额: {weth_balance}")
        except Exception as e:
            result_queue.put((address, 'Error', 'Error'))
            print(f"获取地址 {address} 的余额时出错: {str(e)}")
        
        time.sleep(0.1)  # 避免请求过快

def process_json_file(json_file, progress_file):
    # 读取进度文件
    processed_addresses = set()
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            processed_addresses = set(line.strip() for line in f)
    
    # 读取JSON文件
    with open(json_file, 'r') as f:
        addresses = json.load(f)
    
    # 过滤掉已处理的地址
    addresses = [addr for addr in addresses if addr not in processed_addresses]
    
    if not addresses:
        print(f"文件 {json_file} 中的所有地址都已处理")
        return
    
    # 创建队列
    address_queue = Queue()
    result_queue = Queue()
    
    # 将地址放入队列
    for addr in addresses:
        address_queue.put(addr)
    
    # 创建工作线程
    threads = []
    for _ in range(64):
        t = threading.Thread(target=worker, args=(address_queue, result_queue))
        t.start()
        threads.append(t)
    
    # 等待所有线程完成
    for t in threads:
        t.join()
    
    # 收集结果
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    
    # 更新进度文件
    with open(progress_file, 'a') as f:
        for addr, _, _ in results:
            f.write(f"{addr}\n")
    
    # 读取现有的CSV文件（如果存在）
    csv_file = os.path.join('data', f"{os.path.splitext(os.path.basename(json_file))[0]}.csv")
    existing_data = []
    if os.path.exists(csv_file):
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            existing_data = list(reader)
    
    # 合并数据
    all_data = existing_data + results
    
    # 转换为浮点数并排序
    sorted_data = []
    for row in all_data:
        try:
            eth_balance = float(row[1]) if row[1] != 'Error' else 0.0
            weth_balance = float(row[2]) if row[2] != 'Error' else 0.0
            total_balance = eth_balance + weth_balance
            sorted_data.append([row[0], eth_balance, weth_balance, total_balance])
        except (ValueError, IndexError):
            continue
    
    # 按总余额排序
    sorted_data.sort(key=lambda x: x[3], reverse=True)
    
    # 写入CSV文件
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Address', 'ETH_Balance', 'WETH_Balance', 'Total_Balance'])
        for row in sorted_data:
            writer.writerow(row)
    
    print(f"文件 {json_file} 处理完成，结果已保存到 {csv_file}")

def main():
    # 确保data目录存在
    if not os.path.exists('data'):
        os.makedirs('data')
    
    # 确保progress目录存在
    if not os.path.exists('progress'):
        os.makedirs('progress')
    
    # 获取所有JSON文件
    json_files = [f for f in os.listdir('data') if f.endswith('.json')]
    
    for json_file in json_files:
        json_path = os.path.join('data', json_file)
        progress_file = os.path.join('progress', f"{os.path.splitext(json_file)[0]}_progress.txt")
        
        print(f"开始处理文件: {json_file}")
        process_json_file(json_path, progress_file)

if __name__ == '__main__':
    main() 