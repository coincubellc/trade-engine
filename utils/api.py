from decimal import Decimal as dec
import requests

from database import *

log = logging.getLogger(__name__)

_exapi_url = os.getenv('EXAPI_URL')

GRACE_TIME = 5 # Seconds to sleep on exception
API_RETRIES = 3  # Times to retry query before giving up


def delete_order(cube, order_id):
    try:
        # Delete from database
        del cube.all_orders[order_id]
        db_session.add(cube)
        db_session.commit()
    except KeyError:
        pass


def api_request(cube, request_type, exchange, endpoint, params):
    ex = Exchange.query.filter_by(name=exchange).one()
    url = f'{_exapi_url}/{exchange}{endpoint}'
    r = requests.request(request_type, url, params=params)
    log.debug(r.status_code)
    if r.status_code == 200:
        json_content = r.json()
        return json_content
    if r.status_code == 400:
        return 'InvalidOrder'
    if r.status_code == 503:
        return None 
    if r.status_code == 401: 
        fail_connection(cube, ex)  
        return None    
    if r.status_code == 403: 
        fail_connection(cube, ex)  
        return None                 


def fail_connection(cube, ex):
    log.warning(f'{ex} {cube} Failing API')
    cube.connections[ex.name].failed_at = datetime.utcnow()
    # Delete orphan orders
    for order_id in list(cube.all_orders.keys()):
        try:
            if cube.all_orders[order_id].ex_pair.exchange.name == ex.name:
                delete_order(cube, order_id)
        except KeyError:
            log.debug(f'{cube} Order {order_id} missing from orders')
            continue
    db_session.add(cube)
    db_session.commit()


def get_api_creds(cube, exchange):
    conn = Connection.query.filter_by(cube_id=cube.id, exchange_id=exchange.id).first()
    # API credentials
    creds = {
        'key': conn.decrypted_key,
        'secret': conn.decrypted_secret,
        'passphrase': conn.decrypted_passphrase
    }
    return creds


def get_price(exchange, base, quote):
    url = f'{_exapi_url}/{exchange}/midprice'
    params = {
        'base': base,
        'quote': quote
    }
    r = requests.get(url, params=params)
    if r.status_code == 200:
        price = r.json()
        return dec(price['price_str'])
    else:
        raise r.status_code


def record_api_key_error(cube, ex_name, error):
    exchange = Exchange.query.filter_by(name=ex_name).first()
    try:
        new_error = ConnectionError(
                        user_id=cube.user_id,
                        cube_id=cube.id,
                        exchange_id=exchange.id,
                        error_message=str(error)      
                        )
        db_session.add(new_error)
        db_session.commit()
    except:
        log.debug(error)