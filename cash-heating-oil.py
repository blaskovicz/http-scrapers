from dotenv import load_dotenv
from requests import get
import paho.mqtt.client as mqtt
import re
from os import environ

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*'
              ';q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en,ja;q=0.9,en-US;q=0.8,zh-CN;q=0.7,zh;q=0.6',
    'Accept-Encoding': 'gzip, deflate, br',
    'Cache-Control': 'max-age=0',
    'Connection': 'close',
    'Cookie': '',
    'Upgrade-Insecure-Requests': '1',
    'DNT': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/107.0.0.0 Safari/537.36',
}

cost_re = re.compile(r"<td>\s*\$(\d+\.\d+)\s*</td>")

def main():
    load_dotenv()

    mqtt_topic = environ['mqtt_topic']
    client = mqtt.Client(client_id='scraper-cashheatingoil')
    client.username_pw_set(environ['mqtt_username'], environ['mqtt_password'])
    client.connect(host=environ['mqtt_host'])
    client.loop_start()

    r = get('https://www.cashheatingoil.com/wilton_ct_oil_prices/06897', headers=headers)
    if r.status_code != 200:
        raise f'{r.status_code}: {r.text}'

    cost = None
    for cost_match in cost_re.finditer(r.text):
        raw_cost = cost_match.group(1)
        try:
            current_cost = float(raw_cost)
            cost = min(cost, current_cost) if cost is not None else current_cost
        except ValueError as e:
            print(f'failed to parse {raw_cost}: {e}')

    # influx format https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/
    message = f'fuel,town=wilton,state=ct,zip=06897 price_lowest={cost}'
    res = client.publish(mqtt_topic, message, retain=True)

    res.wait_for_publish(timeout=10)

    print(f'publish {mqtt_topic} {message}')


if __name__ == '__main__':
    main()
