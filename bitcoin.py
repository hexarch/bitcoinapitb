import requests
import time
import psycopg2

connection = psycopg2.connect(
    user="*******",
    password="******",
    host="******",
    port="******",
    database="******"
)
cursor = connection.cursor()

def get_latest_block():
    url = "https://blockchain.info/latestblock"
    response = requests.get(url)
    data = response.json()
    return data

def get_block_transactions(block_hash):
    url = f"https://blockchain.info/rawblock/{block_hash}"
    response = requests.get(url)
    data = response.json()
    return data["tx"]

def extract_wallet_addresses(transactions):
    wallet_addresses = set()
    for transaction in transactions:
        for input in transaction["inputs"]:
            prev_out = input.get("prev_out")
            if prev_out and "addr" in prev_out:
                wallet_addresses.add(prev_out["addr"])
        for output in transaction["out"]:
            if "addr" in output:
                wallet_addresses.add(output["addr"])
    return wallet_addresses

def main():
    latest_block = get_latest_block()
    latest_block_hash = latest_block["hash"]
    print(f"Son blok hash: {latest_block_hash}")
    block_transactions = get_block_transactions(latest_block_hash)
    wallet_addresses = extract_wallet_addresses(block_transactions)
    print("Son blokta kullanılan cüzdan adresleri:")
    z = 1
    for address in wallet_addresses:
        check_query = "SELECT COUNT(*) FROM bitcoingenel WHERE cuzdan = %s"
        cursor.execute(check_query, (address,))
        result = cursor.fetchone()
        if result[0] > 0:
            print(f"Çüzdan zaten mevcut: {address}")
        else:
            insert_query = f"INSERT INTO bitcointb{z} (cuzdan) VALUES (%s)"
            cursor.execute(insert_query, (address,))
            print(f"Veri tabanına kaydedildi: {address}")
            
            insert_queryq = "INSERT INTO bitcoingenel (cuzdan) VALUES (%s)"
            cursor.execute(insert_queryq, (address,))
            connection.commit()
            print(f"Veri tabanına kaydedildi: {address}")
            z += 1
            if z == 11:
                z = 1

if __name__ == "__main__":
    while True:
        try:
            main()
            time.sleep(10)
        except Exception as e:
            print(f"Hata oluştu: {e}")
            continue
