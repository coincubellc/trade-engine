from utils.api import api_request
from datetime import datetime

from database import Cube, Exchange

RUNS = 10

cube = Cube.query.all()[0]
exchanges = Exchange.query.filter_by(active=True).all()

for ex in exchanges:
    last = None
    ex_name = ex.name + '/'  
    print(f'Test api request limit for {ex_name}')
    for i in range(RUNS):
        print('Run number ', i)
        current = datetime.utcnow()
        print(current)
        if last:
            diff = current - last
            print(f'{diff} seconds between queries')
        last = current
        result = api_request(
                    cube,
                    'GET',
                    ex_name,
                    'orderbook',
                    None
                    )
        print(result['asks'][0][0])