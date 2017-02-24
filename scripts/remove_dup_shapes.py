shapes = open("./transit_files/shapes.txt")
i = 0
points = []
truelines = []
for lines in shapes:
    if i != 0:
        xtemp = lines.split(',')[1]
        ytemp = lines.split(',')[2]
        if xtemp + "" + ytemp not in points:
            points.append(xtemp + "" + ytemp)
            truelines.append(lines)
    else:
        truelines.append(lines)
    i = i + 1
newfile = open("./transit_files/newshapes.txt", 'w+')
for line in truelines:
    newfile.write(line)
newfile.close()
