file = "tpcc.txt"

# print byte by byte
a = open(file, "r")

while 1:
    byte_s = a.read(1)
    if not byte_s:
        break
    byte = byte_s[0]
    print(byte)

