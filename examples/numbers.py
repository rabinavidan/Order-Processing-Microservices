


print("+++++ boolean int +++++")
print(True and True)
print(True and False)
print(bool(0))
print(bool(1))

print("+++++ boolean string +++++")
z1 = ""
z2 = "False"
print(bool(z1)) # false
print(bool(z2)) # true

z1 = 15
z2 = 9
print("+++++ condition +++++")
if z1:
    print("z1 is truthy")
else:
    print("z1 is falsy")

if z2:
    print("z2 is truthy")
else:
    print("z2 is falsy")
