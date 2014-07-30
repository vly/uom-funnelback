data_file = open("unimelb-report-1-metadata.txt", encoding='latin-1') # encoding req.
for i in range(6321909):
		data_file.readline()
line = data_file.readline()
counter = 0
while line:
	block = {}
	line = data_file.readline()

	if '[' in line and ']' in line:
		url = line.split('] ')
		if len(url) > 1:
			url = url[1][:-1]
			if '.au' in url:
				block['hostname'] = url.split('.au')[0]
				block['uri'] = url.split('.au')[1]
			elif '.edu/' in url:
				block['hostname'] = url.split('.edu')[0]
				block['uri'] = url.split('.edu')[1]
			if len(block) == 2:
				if block['hostname'] == 'www.unimelb.edu':
					print(counter, block['uri'], block['hostname'])
					break
	counter += 1
	line = data_file.readline()