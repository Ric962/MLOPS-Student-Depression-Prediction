import traceback
import sys

class CustomException(Exception):
    def __init__(self, error_message):
        super().__init__(error_message)          # whatever error is we'll show that only
        self.error_message = self.get_detailed_error_message(error_message)

    @staticmethod
    def get_detailed_error_message(error_message):                  
        _,_, exc_tb = sys.exc_info()
        file_name = exc_tb.tb_frame.f_code.co_filename
        line_number = exc_tb.tb_lineno

        return f"Error in {file_name}, line {line_number} : {error_message}"

    def __str__(self):
        return self.error_message