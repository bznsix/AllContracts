#!/usr/bin/env python3
import asyncio
import csv
import time
import argparse
import os
from typing import List, Dict, Any, Set
import aiohttp
from web3 import Web3
import sys

# 不同网络的代币合约地址配置
NETWORK_CONFIGS = {
    "ethereum": {
        "rpc_nodes": [
            "http://192.168.31.100:8547",
        ],
        "tokens": {
            "WBTC": {"address": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", "decimals": 8},
            "USDC": {"address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", "decimals": 6},
            "USDT": {"address": "0xdAC17F958D2ee523a2206206994597C13D831ec7", "decimals": 6},
            "WETH": {"address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "decimals": 18},
            "ETH": {"decimals": 18}
        }
    },
    "arbitrum": {
        "rpc_nodes": [

        ],
        "tokens": {
            "WBTC": {"address": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f", "decimals": 8},
            "USDC": {"address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "decimals": 6},
            "USDT": {"address": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", "decimals": 6},
            "DAI": {"address": "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1", "decimals": 6},
            "WETH": {"address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "decimals": 18},
            "weETH": {"address": "0x35751007a407ca6FEFfE80b3cB397736D2cf4dbe", "decimals": 18},
            "ezETH": {"address": "0x2416092f143378750bb29b79eD961ab195CcEea5", "decimals": 18},
            "tETH": {"address": "0xd09ACb80C1E8f2291862c4978A008791c9167003", "decimals": 18},
            "rETH": {"address": "0xec70dcb4a1efa46b8f2d97c310c9c4790ba5ffa8", "decimals": 18},
            "ETH": {"decimals": 18}
        }
    },
    "Optimism": {
        "rpc_nodes": [

        ],
        "tokens": {
            "USDC": {"address": "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174", "decimals": 6},
            "WBTC": {"address": "0x1BFD67037B42Cf73acF2047067bd4F2C47D9BfD6", "decimals": 8},
            "USDT": {"address": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F", "decimals": 6},
            "DAI": {"address": "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1", "decimals": 18},
            "WETH": {"address": "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619", "decimals": 18},
            "ezETH": {"address": "0x2416092f143378750bb29b79eD961ab195CcEea5", "decimals": 18},
            "weETH": {"address": "0x5A7fACB970D094B6C7FF1df0eA68D99E6e73CBFF", "decimals": 18},
            "sfrxETH": {"address": "0x484c2D6e3cDd945a8B2DF735e079178C1036578c", "decimals": 18},
            "rETH": {"address": "0x9bcef72be871e61ed4fbbc7630889bee758eb81d", "decimals": 18},
            "frxETH": {"address": "0x6806411765af15bddd26f8f544a34cc40cb9838b", "decimals": 18},
            "MATIC": {"decimals": 18}
        }
    },
    "bsc": {
        "rpc_nodes": [
            "http://192.168.31.100:8545",
        ],
        "tokens": {
            "USDC": {"address": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d", "decimals": 6},
            "USDT": {"address": "0x55d398326f99059fF775485246999027B3197955", "decimals": 6},
            "BUSD": {"address": "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56", "decimals": 6},
            "WBNB": {"address": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", "decimals": 18},
            "WETH": {"address": "0x4DB5a66E937A9F4473fA95b1cAF1d1E1D62E29EA", "decimals": 18},
            "BNB": {"decimals": 18}
        }
    }
}

# ERC20 balanceOf 方法的函数选择器
BALANCE_OF_SELECTOR = "0x70a08231"  # balanceOf(address)

# 批处理和并发设置
BATCH_SIZE = 1000  # 每批处理的地址数量
MAX_CONCURRENT_REQUESTS = 100  # 最大并发请求数

# 连接池和节点选择器
class RPCManager:
    def __init__(self, rpc_urls: List[str], max_connections: int = 100):
        self.rpc_urls = rpc_urls
        self.current_idx = 0
        self.session = None
        self.max_connections = max_connections
        
    async def init_session(self):
        # 创建一个共享的会话，设置最大连接数
        connector = aiohttp.TCPConnector(limit=self.max_connections, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(connector=connector)
        
    async def close_session(self):
        if self.session:
            await self.session.close()
    
    def get_next_url(self):
        url = self.rpc_urls[self.current_idx]
        self.current_idx = (self.current_idx + 1) % len(self.rpc_urls)
        return url
    
    async def make_request(self, method: str, params: List) -> Dict:
        url = self.get_next_url()
        payload = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": method,
            "params": params
        }
        
        try:
            async with self.session.post(url, json=payload) as response:

                print("请求URL:", url)
                print("请求数据:", payload)
                print("响应状态:", response.status)
                if response.status == 429:  # 处理请求过多的错误
                    print("请求过多，等待重试...")
                    await asyncio.sleep(5)  # 等待后重试
                    return await self.make_request(method, params)
                
                if response.status != 200:
                    return {"error": f"HTTP错误 {response.status}"}
                
                result = await response.json()
                if "error" in result:
                    return {"error": result["error"]}                    
                return result
        except Exception as e:
            return {"error": str(e)}

# 获取地址的原生代币余额和其他代币余额
async def get_balances(address: str, rpc_manager: RPCManager, token_config: Dict) -> Dict[str, Any]:
    # 确定原生代币（ETH, MATIC等）基于代币配置
    native_token = next((token for token in token_config if "address" not in token_config[token]), "ETH")
    
    # 初始化结果，包含地址和所有代币余额设为0
    result = {"address": address}
    for token in token_config:
        result[token] = "0"
    
    try:
        # 获取原生代币余额（ETH/MATIC等）
        native_response = await rpc_manager.make_request(
            "eth_getBalance",
            [address, "latest"]
        )
        
        if "result" in native_response and not isinstance(native_response.get("error"), dict):
            native_balance = int(native_response["result"], 16)
            result[native_token] = str(native_balance / 10**token_config[native_token]["decimals"])
            print(f"地址: {address}, 代币: {native_token}, 余额: {result[native_token]}")  # 打印原生代币余额
        
        # 获取每个代币的余额
        for token_symbol, token_info in token_config.items():
            if "address" in token_info:  # 跳过没有合约地址的原生代币
                # 创建 balanceOf 调用数据: 函数选择器 + 地址参数(补齐到32字节)
                address_param = address[2:].lower().zfill(64)
                data = f"{BALANCE_OF_SELECTOR}{address_param}"
                token_response = await rpc_manager.make_request(
                    "eth_call",
                    [{"to": token_info["address"], "data": data}, "latest"]
                )
                print("响应json数据:", token_response)
                if "result" in token_response and not isinstance(token_response.get("error"), dict):
                    balance_hex = token_response["result"]
                    if balance_hex and balance_hex != "0x":
                        balance = int(balance_hex, 16)
                        decimals = token_info["decimals"]
                        result[token_symbol] = str(balance / 10**decimals)
                        print(f"地址: {address}, 代币: {token_symbol}, 余额: {result[token_symbol]}")  # 打印代币余额
                        print( "===========")
        
        return result
    
    except Exception as e:
        with open(args.error_log, "a") as error_file:
            error_file.write(f"地址 {address} 出错: {str(e)}\n")
        return result

# 从CSV获取已经处理过的地址
def get_processed_addresses_from_csv(output_file: str) -> Set[str]:
    processed = set()
    if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
        try:
            with open(output_file, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    processed.add(row['address'])
        except Exception as e:
            print(f"读取已处理地址时出错: {e}")
    return processed

# 将结果写入CSV文件
def write_to_csv(results: List[Dict], file_path: str, token_config: Dict, append: bool = False):
    if not results:  # 如果没有结果，则跳过写入
        return

    mode = 'a' if append else 'w'
    file_exists = os.path.exists(file_path) and os.path.getsize(file_path) > 0
    
    with open(file_path, mode, newline='') as csvfile:
        fieldnames = ['address'] + list(token_config.keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if not append or not file_exists:
            writer.writeheader()
        
        for result in results:
            writer.writerow(result)

# 处理一批地址
async def process_batch(addresses: List[str], rpc_manager: RPCManager, token_config: Dict, 
                       processed_addresses: Set[str], output_file: str, progress_file: str) -> List[Dict]:
    results = []
    # 控制并发请求数量
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async def get_balance_with_semaphore(addr):
        async with semaphore:
            return await get_balances(addr, rpc_manager, token_config)
    # 创建所有查询任务
    for addr in addresses:
        if not Web3.is_address(addr):
            with open(args.error_log, "a") as error_file:
                error_file.write(f"无效地址: {addr}\n")
            continue
        if addr in processed_addresses:
            print(f"跳过已处理的地址: {addr}")
            continue
        # 获取余额
        result = await get_balance_with_semaphore(addr)
        results.append(result)
        # 将结果写入CSV文件
        write_to_csv([result], output_file, token_config, append=True)
        # 将地址写入进度文件
        with open(progress_file, "a") as progress_file_handle:
            progress_file_handle.write(f"{addr}\n")
    return results
# 解析命令行参数

def parse_arguments():
    parser = argparse.ArgumentParser(description='获取地址列表的代币余额')
    parser.add_argument('--addresses', type=str, default='addresses.txt',
                      help='包含地址的文件路径 (默认: addresses.txt)')
    parser.add_argument('--output', type=str, default='balances.csv',
                      help='输出CSV文件的路径 (默认: balances.csv)')
    parser.add_argument('--error-log', type=str, default='errors.log',
                      help='错误日志文件的路径 (默认: errors.log)')
    parser.add_argument('--network', type=str, default='ethereum',
                      choices=NETWORK_CONFIGS.keys(),
                      help='检查余额的网络 (默认: ethereum)')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                      help=f'每批处理的地址数量 (默认: {BATCH_SIZE})')
    parser.add_argument('--max-concurrent', type=int, default=MAX_CONCURRENT_REQUESTS,
                      help=f'最大并发请求数 (默认: {MAX_CONCURRENT_REQUESTS})')
    parser.add_argument('--resume', action='store_true',
                      help='跳过输出CSV中已处理的地址')
    parser.add_argument('--restart', action='store_true',
                      help='重新开始，覆盖现有输出文件')
    parser.add_argument('--progress-file', type=str, default='progress.txt',
                      help='进度文件的路径 (默认: progress.txt)')
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
        
    return parser.parse_args()

# 从进度文件获取已处理的地址
def get_processed_addresses_from_progress(progress_file: str) -> Set[str]:
    processed = set()
    if os.path.exists(progress_file) and os.path.getsize(progress_file) > 0:
        try:
            with open(progress_file, 'r') as file:
                for line in file:
                    processed.add(line.strip())
        except Exception as e:
            print(f"读取进度文件时出错: {e}")
    return processed

# 将地址写入进度文件
def write_address_to_progress(address: str, progress_file: str):
    with open(progress_file, 'a') as file:
        file.write(f"{address}\n")

# 处理一批地址
async def process_batch(addresses: List[str], rpc_manager: RPCManager, token_config: Dict, 
                       processed_addresses: Set[str], output_file: str, progress_file: str) -> List[Dict]:
    results = []
    pending_tasks = []
    
    # 控制并发请求数量
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    async def get_balance_with_semaphore(addr):
        async with semaphore:
            return await get_balances(addr, rpc_manager, token_config)
    
    # 创建所有查询任务
    for addr in addresses:
        if not Web3.is_address(addr):
            with open(args.error_log, "a") as error_file:
                error_file.write(f"无效地址: {addr}\n")
            continue
        
        if addr in processed_addresses:
            print(f"跳过已处理的地址: {addr}")
            continue
        
        pending_tasks.append(get_balance_with_semaphore(addr))
    
    # 如果没有待处理任务，直接返回空结果
    if not pending_tasks:
        return []
    
    # 等待所有任务完成
    results = await asyncio.gather(*pending_tasks)

    # 将结果写入CSV文件
    write_to_csv(results, output_file, token_config, append=True)

    # 写入进度文件
    for result in results:
        write_address_to_progress(result["address"], progress_file)

    return results

# 主函数
async def main():
    global args
    args = parse_arguments()
    
    # 如果通过命令行提供，则更新全局设置
    global BATCH_SIZE, MAX_CONCURRENT_REQUESTS
    BATCH_SIZE = args.batch_size
    MAX_CONCURRENT_REQUESTS = args.max_concurrent
    
    # 获取网络配置
    network_config = NETWORK_CONFIGS[args.network]
    token_config = network_config["tokens"]
    
    # 初始化RPC管理器和会话
    rpc_manager = RPCManager(network_config["rpc_nodes"], MAX_CONCURRENT_REQUESTS * 2)
    await rpc_manager.init_session()
    
    try:
        # 读取地址文件
        with open(args.addresses, 'r') as f:
            all_addresses = [addr.strip() for addr in f.readlines()]
        
        valid_addresses = [addr for addr in all_addresses if Web3.is_address(addr)]
        
        # 获取已处理的地址（如果是续传模式）
        processed_addresses = set()
        if args.resume and not args.restart:
            processed_addresses = get_processed_addresses_from_csv(args.output)
            print(f"找到{len(processed_addresses)}个已处理的地址")
        
        # 从进度文件获取已处理的地址
        processed_addresses.update(get_processed_addresses_from_progress(args.progress_file))
        
        # 如果要重新开始
        if args.restart:
            # 创建新的输出文件，包含表头
            with open(args.output, 'w', newline='') as csvfile:
                fieldnames = ['address'] + list(token_config.keys())
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
            
            # 清空错误日志
            with open(args.error_log, 'w') as error_file:
                error_file.write('')
            
            # 清空进度文件
            with open(args.progress_file, 'w') as progress_file:
                progress_file.write('')
            
            print("重新开始，已清除之前的数据")
        
        print(f"将处理{len(valid_addresses)}个有效地址，共{len(all_addresses)}个地址")
        print(f"网络: {args.network}")
        print(f"代币: {', '.join(token_config.keys())}")
        
        # 按批次处理地址
        start_time = time.time()
        
        # 为了更好的显示进度，先计算待处理地址数量
        pending_addresses = [addr for addr in valid_addresses if addr not in processed_addresses]
        total_pending = len(pending_addresses)
        
        print(f"有{total_pending}个地址待处理")
        
        processed_in_session = 0
        
        # 按批次处理地址
        for i in range(0, len(valid_addresses), BATCH_SIZE):
            batch_addresses = valid_addresses[i:i + BATCH_SIZE]
            
            # 计算这个批次中有多少个地址需要处理
            pending_in_batch = sum(1 for addr in batch_addresses if addr not in processed_addresses)
            
            if pending_in_batch == 0:
                print(f"批次{i//BATCH_SIZE + 1}中的所有地址已处理，跳过")
                continue
                
            print(f"处理批次{i//BATCH_SIZE + 1}/{(len(valid_addresses) + BATCH_SIZE - 1)//BATCH_SIZE}，"
                  f"其中有{pending_in_batch}个地址待处理")
            
            # 处理这个批次
            batch_results = await process_batch(
                batch_addresses, rpc_manager, token_config, processed_addresses, args.output, args.progress_file
            )
            
            # 更新已处理的地址
            processed_in_session += len(batch_results)
            for result in batch_results:
                processed_addresses.add(result["address"])
            
            # 进度报告
            elapsed = time.time() - start_time
            progress = processed_in_session / total_pending if total_pending > 0 else 1.0
            remaining = (elapsed / progress - elapsed) if progress > 0 else 0
            
            print(f"本次运行已处理: {processed_in_session}/{total_pending}, "
                  f"总进度: {len(processed_addresses)}/{len(valid_addresses)}, "
                  f"已用时间: {elapsed:.1f}秒, 预计剩余时间: {remaining:.1f}秒")
            
            # 短暂延迟以避免过度占用资源
            await asyncio.sleep(0.5)
        
        print(f"处理完成！结果已保存到{args.output}")
    
    finally:
        # 关闭会话
        await rpc_manager.close_session()

if __name__ == "__main__":
    asyncio.run(main())