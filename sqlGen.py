import random
for i in range (10):
	print ("r0: INSERT INTO student values(" + str(i) + ",'devin','devin.smith'," + str(random.randint(1,12)) + ");")
for j in range (1000):
	print ("r0: BEGIN;")
	print ("r0: SELECT * FROM student WHERE grade = " + str(random.randint(1,12)) + ";")
	print ("r0: COMMIT;")
