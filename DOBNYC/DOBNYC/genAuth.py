from Crypto.Cipher import AES
from Crypto.Util.Padding import pad
from base64 import b64encode
import datetime
import moment

"""
Filename: getAuth.py

Description:
This script generate authentication tokens and userbrowserid in headers for https://a810-dobnow.nyc.gov based on formatted dates.

Date: 2024/07/07

Functions:
- encrypt_msg(message, key): Encrypts the provided message using AES encryption.
- generate_tokens(): Generates authentication tokens based on formatted UTC dates.

"""


def encrypt_msg(message, key):
    # Convert the key and message to bytes
    key_bytes = key.encode('utf-8')
    message_bytes = message.encode('utf-8')
    
    # Encrypt the message using the given key and IV
    cipher = AES.new(key_bytes, AES.MODE_CBC, iv=key_bytes)
    padded_message = pad(message_bytes, AES.block_size, style='pkcs7')
    encrypted_message = cipher.encrypt(padded_message)
    return encrypted_message

def generate_tokens():
    # Set the key (must be 16 bytes)
    key_token = "5A484407-F43E-E7".ljust(16)[:16]
    key_session = "5A484407-F43E-E6".ljust(16)[:16]

    # Get the current time and format it
    now = datetime.datetime.utcnow()
    utc_string = now.strftime("%a, %d %b %Y %H:%M:%S GMT")
    utc_hours = now.hour

    # Format the date string
    formatted_date = moment.date(utc_string).format("MM/DD/YYYY")
    encrypted_token = encrypt_msg(formatted_date, key_token)
    auth_token_base64 = b64encode(encrypted_token).decode('utf-8')

    ## Translate comments for formatted UTC date and hour string
    formatted_utc = moment.date(utc_string).format("DD/MM/YYYY") + "|" + str(utc_hours)
    encrypted_session_token = encrypt_msg(formatted_utc, key_session)
    session_token_base64 = b64encode(encrypted_session_token).decode('utf-8')

    return auth_token_base64, session_token_base64