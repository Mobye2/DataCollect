import time
import datetime
# handle api limit error
def api_upper_limit_error(func):
    def wrapper(*args, **kwargs):
        while True:
            try:
                output=func(*args, **kwargs)
                break

            except Exception as e:
                print('error happen,reason:', e)
                if 'upper limit' in str(e).lower():
                    print('upper limit, wait for 60 minutes')
                    print (datetime.datetime.now())
                    time.sleep(3600)
                    continue
                else:
                    print('other error,pass')
                    break
        return output
    return wrapper
