from web3 import Web3
import csv
from datetime import datetime
import threading
from queue import Queue
import time

# 连接到以太坊主网
eth = "http://192.168.31.100:8547" # Alchemy免费节点
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
        
        # time.sleep(0.1)  # 避免请求过快

# 读取CSV文件并添加ETH和WETH余额
input_file = 'address.csv'
output_file = f'address.csv'

# 创建队列
address_queue = Queue()
result_queue = Queue()

# 读取地址到队列
with open(input_file, 'r') as infile:
    reader = csv.reader(infile)
    header = next(reader)  # 跳过标题行
    for row in reader:
        if row and '0x' in row[0]:
            address = row[0].split(',')[0].strip()
            address_queue.put(address)

# 创建4个工作线程
threads = []
for _ in range(64):
    t = threading.Thread(target=worker, args=(address_queue, result_queue))
    t.start()
    threads.append(t)

# 等待所有线程完成
for t in threads:
    t.join()

# 收集结果并写入文件
results = []
while not result_queue.empty():
    results.append(result_queue.get())

# 写入结果到CSV
with open(output_file, 'w', newline='') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(['Address', 'ETH_Balance', 'WETH_Balance'])
    for result in results:
        writer.writerow(result)

print(f"处理完成。结果已保存到 {output_file}")

# 读取并排序ETH余额数据
sorted_file = 'address.csv'
sorted_output = 'address_sort.csv'

with open(sorted_file, 'r') as infile:
    reader = csv.reader(infile)
    header = next(reader)  # 跳过标题行
    
    # 读取所有数据并转换余额为浮点数
    data = []
    for row in reader:
        if len(row) >= 3:  # 确保行至少有地址、ETH余额和WETH余额三列
            try:
                eth_balance = float(row[1])
                weth_balance = float(row[2])
                total_balance = eth_balance + weth_balance
                data.append([row[0], eth_balance, weth_balance, total_balance])
            except ValueError:
                data.append([row[0], 0.0, 0.0, 0.0])

    # 按总余额(ETH+WETH)降序排序
    sorted_data = sorted(data, key=lambda x: x[3], reverse=True)

    # 写入排序后的数据
    with open(sorted_output, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Address', 'ETH_Balance', 'WETH_Balance', 'Total_Balance'])  # 写入标题行
        for row in sorted_data:
            writer.writerow(row)

print(f"已按总余额(ETH+WETH)排序并保存到 {sorted_output}")
