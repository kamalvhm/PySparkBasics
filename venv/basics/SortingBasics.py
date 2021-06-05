lst =[4,2,3,1]

def copyRange(lst,start,end):
    start =int(start)
    end=int(end)
    newList=[]
    for i in range(start,end):
        newList.append(lst[i])
    return newList

def swap(lst,i,j):
    temp=lst[i]
    lst[i]=lst[j]
    lst[j]=temp

def selectionsort(lst):
    for i in range(len(lst)):
        swpIndex=i
        for j in range(i+1,len(lst)):
            if lst[j]<lst[swpIndex]:
                swpIndex=j
        swap(lst,i,swpIndex)

def bubbleSort(lst):
    for k in range(1,len(lst)):
        flag=True
        for i in range(len(lst)-k):
            if lst[i]>lst[i+1]:
                flag=False
                swap(lst,i,i+1)
        if flag:
            break

def insersionSort(lst):
    for i in range(len(lst)):
        value=lst[i]
        hole=i
        while hole > 0 and lst[hole-1]>value:
            lst[hole]=lst[hole-1]
            hole-=1
        lst[hole]=value

def merge(lst,left,right):
    n=len(left)
    m=len(right)
    i,j,k=0,0,0
    while i<n and j<m:
        if left[i]<right[j]:
            lst[k]=left[i]
            i+=1
            k+=1
        else:
            lst[k]=right[j]
            j+=1
            k+=1
    while(i<n):
        lst[k] = left[i]
        i += 1
        k += 1
    while j<m:
        lst[k] = right[j]
        j += 1
        k += 1
    return lst


def mergeSort(lst):
    n=len(lst)
    if n<=1:
        return lst
    else :
        left=mergeSort(copyRange(lst,0,n/2))
        right=mergeSort(copyRange(lst,n/2,n))
        return merge(lst,left,right)


print("Before",lst)
mergeSort(lst)
print("after",lst)
n=len(lst)
print(n)
