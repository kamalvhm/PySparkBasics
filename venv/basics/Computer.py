class Robot:#https://www.youtube.com/watch?v=WOwi0h_-dfA&list=PLBZBJbE_rGRWeh5mIBhD-hhDwSEDxogDg&index=10
    def __init__(self,n,c,w):
        self.name=n
        self.color=c
        self.weight=w

    def introduce_self(self):
        print("My name is "+self.name)

r1=Robot("Tom","red",30)
r2=Robot("Jerry","blue",40)

class Person:
    def __init__(self,n,p,i):
        self.name=n
        self.personality=p
        self.is_sitting=i

    def sit_down(self):
        self.is_sitting=True

    def stand_up(self):
        self.is_sitting = False

p1=Person("Alice","Aggesive",False)
p2=Person("Becky","talkative",True)

#p1 person owns r2 robot
p1.robot_owned=r2
p2.robot_owned=r1

p1.robot_owned.introduce_self()
