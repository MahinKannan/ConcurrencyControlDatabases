import copy
import sys

#outputfile = open("check.txt",'w')		#Output file for the program
								#Uncomment last line (file close) too
#sys.stdout = outputfile				#Use this to print only to file and not terminal

def printTwo(*args):
	print(*args)
	# printThis = ""					#Uncomment these lines (10 - 15) to write to file.
	# for i in range(len(args)):
	# 	printThis = printThis + str(args[i])

	# #outputfile.write(printThis)				
	# #outputfile.write('\n')						

operations = []
####Change the input file below######################################################

with open('input6.txt','r') as fp:			#Read input file here

#####################################################################################
	for line in fp.readlines():
		line = line.strip().replace(' ','')
		operations.append(line)
#printTwo(operations)
if len(operations) == 1:					#if input file format is b1;b2;b3 (single line)
	operations = operations[0].split(';')		
	del(operations[-1])
else:										#else if each operation is in a separate line
	operations = [op.split(';')[0] for op in operations if op]
printTwo("Operations read from the input file: ",operations)

transaction_table = []				#Transaction Table
lock_table = []						#Lock table
timestamp = 0						#Timestamp

lock_table.append(['dummyVal','none',[]])

def timeStampGetter(x):						#Get timestamp from the transaction table
	global transaction_table
	for i in range(len(transaction_table)):
		if transaction_table[i][0]==x:
			return transaction_table[i][1]

def start_new_transaction(op):
#		Takes the operation as the argument and starts a new transaction
	global timestamp
	global transaction_table
	timestamp += 1
	record = [int(op[1]), timestamp, 'active', [], []]
	transaction_table.append(record)
	printTwo("Transaction started: ",op[1])

def transaction_wants_to_read(op):
	global transaction_table
	global lock_table
	transaction_id = int(op[1])
	item = op.split('(')[1][0]
	printTwo("Transaction that wants to read and item: "+op[1]+' '+item)
	for i in range(len(transaction_table)):
		if transaction_table[i][0] == transaction_id:
			index_in_t_table = i 			#get index of transaction in transaction table
	if transaction_table[index_in_t_table][2]!='waiting' and transaction_table[index_in_t_table][2]!='aborted': 
		#If transaction is not aborted or waiting, continue (active)
		in_lock_table = 0
		for i in range(len(lock_table)):
			if lock_table[i][0]==item:
				index_in_lock_table = i 			#get index of item in lock table
				in_lock_table = 1
				break
		if in_lock_table:						#if item in lock table
			if lock_table[index_in_lock_table][1] == 'read_lock':		#if item has read lock
				
				if transaction_id not in lock_table[index_in_lock_table][2]:		
					lock_table[index_in_lock_table][2].append(transaction_id)
					printTwo("Adding transaction to item readlock list:"+ op[1]+' '+item)		#add to list of transactions holding readlock
			else:														# if item has write lock

				ti = transaction_id
				tj = lock_table[index_in_lock_table][2][0]
				if timeStampGetter(ti) < timeStampGetter(tj):			#wound wait protocol
					abort(tj)
					if len(lock_table[index_in_lock_table][2]) == 0:
						if lock_table[index_in_lock_table][0] == '':
							record = [item, 'read_lock', [transaction_id]]
							lock_table.append(record)
							printTwo("Adding transaction with read lock: ",op[1])
						else:
							lock_table[index_in_lock_table][2].append(ti)	
							lock_table[index_in_lock_table][1] = 'read_lock'
					else:
						lock_table[index_in_lock_table][2][0] = ti	
						lock_table[index_in_lock_table][1] = 'read_lock'
				else:
					wait(op,tj)		
		else:														#if not in lock table create new record.	
			record = [item, 'read_lock', [transaction_id]]
			printTwo("Adding transaction with read lock: ",op[1])
			lock_table.append(record)
	elif transaction_table[index_in_t_table][2]=='waiting':
		printTwo("Adding operation to waitlist of: ",op[1])
		transaction_table[index_in_t_table][3].append(op)
	elif transaction_table[index_in_t_table][2]=='aborted':
		printTwo("Transation already aborted", op[1])

def transaction_wants_to_write(op):
	printTwo("Transaction that wants to write: ",op[1])
	global transaction_table
	global lock_table
	transaction_id = int(op[1])
	item = op.split('(')[1][0]
	for i in range(len(transaction_table)):
		if transaction_table[i][0] == transaction_id:
			index_in_t_table = i
	if transaction_table[index_in_t_table][2]!='waiting' and transaction_table[index_in_t_table][2]!='aborted':
		in_lock_table = 0
		for i in range(len(lock_table)):
			if lock_table[i][0]==item:
				index_in_lock_table = i
				in_lock_table = 1
				break
		if in_lock_table:												#if item in lock table
			if lock_table[index_in_lock_table][1] == 'read_lock':			#if item readlocked
				if len(lock_table[index_in_lock_table][2])==1:
					if transaction_id == lock_table[index_in_lock_table][2][0]:
						lock_table[index_in_lock_table][1] = 'write_lock'
						printTwo("Upgrading readlock to writelock for transaction:", op[1])			#upgrade readlock to writelock
					else:
						ti = transaction_id
						tj = lock_table[index_in_lock_table][2][0]
						if timeStampGetter(ti) < timeStampGetter(tj):
							abort(tj)
							lock_table[index_in_lock_table][2][0] = ti	
							lock_table[index_in_lock_table][1] = 'write_lock'
						else:
							wait(op,tj)
				else:																#if many transactions holding readlock
					#if transaction_id in lock_table[index_in_lock_table][2]:
					t_holding_locks = copy.deepcopy(lock_table[index_in_lock_table][2])
					for i in range(len(lock_table[index_in_lock_table][2])):
						if t_holding_locks[i] != transaction_id:				#woundwait protocol
							ti = transaction_id
							tj = t_holding_locks[i]
							if timeStampGetter(ti) < timeStampGetter(tj):
								abort(tj)
							else:
								wait(op,tj)  
					if len(lock_table[index_in_lock_table][2]) == 1 and lock_table[index_in_lock_table][2][0]==transaction_id:
						lock_table[index_in_lock_table][2][0] = ti	
						lock_table[index_in_lock_table][1] = 'write_lock'
						printTwo("Upgrading to writelock after wound wait for:", op[1])
			else:
				if lock_table[index_in_lock_table][1] == 'write_lock':
					if lock_table[index_in_lock_table][2][0] == transaction_id:
						printTwo("Already has writelock: ",op[1])
					else:
						ti = transaction_id
						tj = lock_table[index_in_lock_table][2][0]
						if timeStampGetter(ti) < timeStampGetter(tj):
							abort(tj)
							if len(lock_table[index_in_lock_table][2]) == 0:
								if lock_table[index_in_lock_table][0] == '':
									record = [item, 'write_lock', [transaction_id]]
									printTwo("Adding transaction with write lock: ",op[1])
									lock_table.append(record)
								else:
									lock_table[index_in_lock_table][2].append(ti)	#Added here
									lock_table[index_in_lock_table][1] = 'write_lock'
							else:
								lock_table[index_in_lock_table][2][0] = ti	#Added here
								lock_table[index_in_lock_table][1] = 'write_lock'
						else:
							wait(op,tj)
		else:
			record = [item, 'write_lock', [transaction_id]]								#Add new record to lock table
			printTwo("Added new write lock for item: " +op[1]+' ' +item)
			lock_table.append(record)
	elif transaction_table[index_in_t_table][2]=='waiting':
		printTwo("Adding operation to waitlist of: ",op[1])
		transaction_table[index_in_t_table][3].append(op)
	elif transaction_table[index_in_t_table][2]=='aborted':
		printTwo("Transaction already aborted", op[1])

def abort(x):								#Abort transaction and change its state
	global transaction_table
	global lock_table
	printTwo("Aborting transaction: ",x)
	transaction_id = x
	for i in range(len(transaction_table)):
		if transaction_table[i][0] == transaction_id:
			index_in_t_table = i
	if transaction_table[index_in_t_table][2]=='aborted':
		printTwo("Already aborted: ",x)
		return
	elif transaction_table[index_in_t_table][2]=='committed':
		printTwo("Already commited: ",x)
		return
	transaction_table[index_in_t_table][3] = []
	for i in range(len(lock_table)):						#release all locks
		if x in lock_table[i][2]:
			itemm = lock_table[i][0]
			if lock_table[i][1] == 'read_lock':
				
				if len(lock_table[i][2]) == 1:
					lock_table[i] = ['','',[]]
				else:
					lock_table[i][2].remove(transaction_id)
				printTwo("Released readlock of: "+ str(x)+ " on item:"+str(itemm))
			if lock_table[i][1] == 'write_lock':
				lock_table[i] = ['','',[]]
				printTwo("Released writelock of: "+ str(x)+ " on item:"+str(itemm))
	transaction_table[index_in_t_table][2]='aborted'
	for i in range(len(transaction_table)):					#Execute waitlist of transactions that were waiting on this transaction
		waitlist = []
		if x in transaction_table[i][4]:
			if transaction_table[i][2]=='committed' or transaction_table[i][2]=='aborted':
				continue
			for k in range(len(transaction_table[i][4])):
				if x in transaction_table[i][4]:
					transaction_table[i][4].remove(x)
			for j in range(len(transaction_table[i][3])):
				if transaction_table[i][3][j][-1] ==')':
					waitlist.append(transaction_table[i][3][j])
					transaction_table[i][3][j]=''
				elif int(transaction_table[i][3][j][-1]) == x:
					waitlist.append(transaction_table[i][3][j])
					transaction_table[i][3][j]=''
			transaction_table[i][2] = 'active'
			printTwo(waitlist)
			for op in waitlist:
				printTwo("Executing waitlist of:", op[1])				
				if op[0]=='r':
					transaction_wants_to_read(op)
				if op[0]=='w':
					transaction_wants_to_write(op)
			if len(transaction_table[i][4])!=0:
					transaction_table[i][2] = 'waiting'

def commit(op):						#Commit transaction and change its state
	global transaction_table
	global lock_table
	x = int(op[1])
	transaction_id = x
	for i in range(len(transaction_table)):
		if transaction_table[i][0] == transaction_id:
			index_in_t_table = i
	if transaction_table[index_in_t_table][2]=='aborted':
		printTwo("Already aborted: ",op[1])
		return
	transaction_table[index_in_t_table][3] = []
	for i in range(len(lock_table)):								#Release all locks
		if x in lock_table[i][2]:
			itemm = lock_table[i][0]
			if lock_table[i][1] == 'read_lock':
				if len(lock_table[i][2]) == 1:
					lock_table[i] = ['','',[]]
				else:
					lock_table[i][2].remove(transaction_id)
				printTwo("Released readlock of: "+ str(x)+ " on item:"+str(itemm))
			if lock_table[i][1] == 'write_lock':
				lock_table[i] = ['','',[]]
				printTwo("Released writelock of: "+ str(x)+ " on item:"+str(itemm))
	printTwo("Committed transaction: ",x)

	for i in range(len(transaction_table)):										#Execute waitlist of transactions that were waiting on this transaction
		waitlist = []
		if x in transaction_table[i][4]:
			if transaction_table[i][2]=='committed' or transaction_table[i][2]=='aborted':
				continue
			for k in range(len(transaction_table[i][4])):
				if x in transaction_table[i][4]:
					transaction_table[i][4].remove(x)
			for j in range(len(transaction_table[i][3])):
				if transaction_table[i][3][j][-1] ==')':
					waitlist.append(transaction_table[i][3][j])
					transaction_table[i][3][j]=''
				elif int(transaction_table[i][3][j][-1]) == x: #check if you need to remove all and make it active
					waitlist.append(transaction_table[i][3][j])
					transaction_table[i][3][j]=''
			transaction_table[i][2] = 'active'
			printTwo(waitlist)
			for op in waitlist:
				printTwo("Executing waitlist of:", op[1])
				if op[0]=='r':
					transaction_wants_to_read(op)
				if op[0]=='w':
					transaction_wants_to_write(op)#if all trans in waitlist done, change stte maybe
			if len(transaction_table[i][4])!=0:
				transaction_table[i][2] = 'waiting'
	transaction_table[index_in_t_table][2]='committed'

def wait(op, tj):							#	Add transaction to waitlist and change its state
	global transaction_table
	global lock_table
	printTwo("Adding operation to waitlist of transaction: ",op[1])
	transaction_id = int(op[1])
	for i in range(len(transaction_table)):
		if transaction_table[i][0] == transaction_id:
			index_in_t_table = i
	transaction_table[index_in_t_table][2] = 'waiting'
	transaction_table[index_in_t_table][3].append(op+str(tj))
	transaction_table[index_in_t_table][4].append(tj)

for count,op in enumerate(operations):	#go through each operation
	printTwo()
	printTwo("Operation :",op)
	if  ['','',[]] in lock_table:
		lock_table.remove(['','',[]])
	if op[0]=='b':
		start_new_transaction(op)
	if op[0]=='r':
		transaction_wants_to_read(op)
	if op[0]=='w':
		transaction_wants_to_write(op)
	if op[0]=='e':
		commit(op)

printTwo("\nTransaction table:")
for i in range(len(transaction_table)):
	printTwo(transaction_table[i])

#outputfile.close()