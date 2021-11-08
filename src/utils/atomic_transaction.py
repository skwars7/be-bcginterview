import logging

class AtomicTransaction(object):

    def __init__(self, rds_helper, callback=None, callback_params={}):
        self.rds_helper = rds_helper
        self.callback = callback
        self.callback_params = callback_params

    def __enter__(self):
        self.conn = self.rds_helper.connection
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exception_type, exception_value, traceback):
        # Handle if exception occurred
        if exception_type:
            logging.info("Rolling back transaction")
            logging.exception(exception_value)
            self.conn.rollback()
            # Call callback
            if self.callback:
                logging.info("Calling callback on rollback")
                self.callback(**self.callback_params)
        else:
            # Commit transaction
            logging.info("Commiting transaction")
            self.conn.commit()
        self.cursor.close()
