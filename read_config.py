def read_config():
    with open('db.config', 'r') as file:
        HOST = file.readline().split(':')[1].strip()
        PORT = int(file.readline().split(':')[1].strip())
        DB   = file.readline().split(':')[1].strip()
        USER = file.readline().split(':')[1].strip()
        PASSWORD = file.readline().split(':')[1].strip()
    return (HOST, PORT, DB, USER, PASSWORD)
