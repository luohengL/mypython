length = int(input())
for _ in range(length):
    num = int(input())
    a = num // 3600
    b = num % 3600 // 60
    c = num % 3600 % 60
    print('%s:%s:%s'%(a,b,c))
    print('%02d:%04d:%02d' % (a, b, c))

