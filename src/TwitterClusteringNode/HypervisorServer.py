import HTTPListener
import sys
import os
import BaseHTTPServer
import libvirtmod as virt
import pycurl
import cStringIO
import json
HOST_NAME = ""

PORT = 9001
VM_TYPES_FILEPATH = ""
PM=[] #Physical Machines
VMID = 1
VM_IMG =[]

def get_all_vm_types():
    fin = open(VM_TYPES_FILEPATH)
    return fin.read()

def loadPMs():
    fin = open("machines")
    i = 0 
    for line in fin.readlines():
        print line
        temp_dict = {}
        ip_address = line.split("@")[-1]
	vms = []
	user = line.split("@")[0]
	temp_dict['pmid'] = i
	i+=1
	temp_dict['ipaddress'] = ip_address[:-1]
	temp_dict['user'] = user
	temp_dict['vms'] = vms
	PM.append(temp_dict)

def get_hypervisor_response(request):
    
    buf = cStringIO.StringIO()
    c = pycurl.Curl()
    curl_request = request
    c.setopt(c.URL, curl_request)
    c.setopt(c.WRITEFUNCTION, buf.write)
    c.perform()
    ret_val = buf.getvalue()
    buf.close()
    return ret_val


def create_new_vm(parameters):
    global PM,VMID,VM_IMG
    all_types = json.loads(get_all_vm_types())
    name = parameters['name'][0]
    vm_req_type = parameters['vm_type'][0]
    image = parameters['itype'][0]
    image_name = ""
    error = 0
    for vm_image in VM_IMG:
        if int(vm_image['id']) == int(image):
	    image_name = vm_image['name']
    if image_name == "":
        error  = 1
    for vm_type in all_types[u'types']:
        if int(vm_type[u'tid'] ) == int(vm_req_type):
	    ram_reqd = int(vm_type[u'ram'])
	    disk_reqd = int(vm_type[u'disk'])
	    cpu_reqd = int(vm_type[u'cpu'])
    min_diff = 1000000000
    pm_select = None
    for pm in PM:
	ip_address = pm['ipaddress']
	pmid = pm['pmid']
	request = "/pm_internal/query?pmid="+str(pmid)+"&vmid=1"
	curl_request= ip_address+request
	pm_resources = json.loads(get_hypervisor_response(curl_request))
	if int(pm_resources[u'free'][u'cpu']) >= cpu_reqd: 
	    if int(pm_resources[u'free'][u'disk']) >= disk_reqd:
	        if int(pm_resources[u'free'][u'ram']) > ram_reqd:
		    if int(pm_resources[u'free'][u'ram']) < min_diff:
		        min_diff = int(pm_resources[u'free'][u'ram'])
			pm_select = pm
    bash_command = "BOOOOO!"
    if not pm_select== None:
	ipaddress = pm_select['ipaddress']
        ### Check for presence of image on Hypervisor
	is_present = "NO"
	if image_name:
            vm_image_request = "/image_internal/present?image_name="+image_name
	    is_present = get_hypervisor_response(ipaddress+vm_image_request)
	else :
	    error =1 
	if not is_present == "YES":
	    print "Transfering File"
	    bash_command = "scp /var/libvirt/images/"+image_name+" "+pm_select['user']+"@"+pm_select['ipaddress'].split(":")[0]+":"+is_present
	    ret_val = os.system(bash_command)
	    print bash_command
	    #print ret_val
	    #if not ret_val==0:
	    #    error = 1
	        

	request = "/vm_internal/create?name=%s&instance_type=%s&image_type=%s&image_name=%s"%(name, vm_req_type,image,image_name)
	request+="&vmid="+str(VMID)
	request= ipaddress+request
	response_code = get_hypervisor_response(request)
	vmid_local = 0+VMID
	if not response_code=="1":
	    vmid_local = 0 
        json_ret_val = "{\n"
	json_ret_val += '"vmid":'+str(vmid_local)+"\n}\n"
	pm_select['vms'].append(vmid_local)
	VMID += 1
	
    else:
        json_ret_val = '{\n"vmid":0\n}\n'
    if error == 1:
        json_ret_val = '{\n"vmid":0\n}\n'
    return json_ret_val


def list_vms(pmid):
    json_ret_val = "{\n\"vmids\":"
    for pm in PM:
        if int(pm['pmid']) == int(pmid):
	    json_ret_val += str(pm['vms'])
    json_ret_val += '\n}\n'
    return json_ret_val
	    
	    
    


def query_vm(parameters):
    vmid = parameters['vmid'][0]
    response = "NOT FOUND"
    for pm in PM:
        print pm['vms']
        if int(vmid) in pm['vms']:
	    curl_request = pm['ipaddress']+"/vm_internal/query?vmid="+str(vmid)
	    response = get_hypervisor_response(curl_request)
    return response
        
def destroy_vm(parameters):
    vmid = parameters['vmid'][0]
    response = "NOT FOUND"
    for pm in PM:
        print pm['vms']
        if int(vmid) in pm['vms']:
	    curl_request = pm['ipaddress']+"/vm_internal/destroy?vmid="+str(vmid)
	    pm['vms'].remove(int(vmid))
	    response = get_hypervisor_response(curl_request)
    return response

def get_pm_list():
    list_of_pms ="["
    for pm in PM[:-1]:
        list_of_pms+=str(pm['pmid'])+","
    list_of_pms += str(PM[-1]['pmid'])+"]"
    json_ret_val = '{\n"pmids" : %s \n}\n'%list_of_pms
    return json_ret_val

def query_pm(parameters):
    response = "Physical Machine configuration unavailable"
    for pm in PM:
        if int(pm['pmid']) == int(parameters) :
	    ip_address = pm['ipaddress']
	    request = "/pm_internal/query?pmid="+str(pm['pmid'])
	    curl_request = ip_address+request
	    response = get_hypervisor_response(curl_request)
    return response

def update_image_list():
    fin = open("images")
    for line in fin.readlines():
        bash_command = "scp "+line[:-1]+" /var/libvirt/images/."
	print bash_command
	os.system(bash_command)
    images = os.listdir("/var/libvirt/images")
    i = 1
    global VM_IMG
    VM_IMG = []
    for image in images:
        temp_dict = {}
	temp_dict['id'] = 1
	temp_dict['name'] = image
	VM_IMG.append(temp_dict)

def get_list_images():
    json_ret_val ='{\n"images":['
    for vm in VM_IMG:
        
        json_ret_val += "{"
	for key in vm.keys()[:-1]:
	    json_ret_val += '"%s":"%s",'%(key,vm[key])
	key = vm.keys()[-1]
        json_ret_val += '"%s":"%s"}'%(key,vm[key])
	
	   
        
    json_ret_val+="]\n}\n"
    return json_ret_val

def get_pub_key():
    rsa_file = "id_rsa.pub"
    fin = open(rsa_file,"r")
    key = fin.read()
    return key

class MasterHTTPServer(HTTPListener.MyHTTPHandler):
    def process_request(self, absolute_path, get_param):
	print get_param
        response = "Master Server unable to handle URI"
	if absolute_path == "/vm/types":
	    response = get_all_vm_types()
	elif absolute_path == "/vm/create":
	    response = create_new_vm(get_param)
	elif absolute_path == "/vm/query":
	    response = query_vm(get_param)
	elif absolute_path == "/vm/destroy":
	    response = destroy_vm(get_param)
	elif absolute_path == "/pm/list":
	    response = get_pm_list()
	elif absolute_path.split("/")[1] == "pm" and absolute_path.split("/")[-1] == "listvms" : 
	    response = list_vms(absolute_path.split("/")[2])
	elif absolute_path.split("/")[1] == "pm":
	    response = query_pm(absolute_path.split("/")[-1])
	elif absolute_path == "/image/list":
	    response = get_list_images()
	    

	elif absolute_path == "/vm_extra/images_update":
	    response = update_image_list()
	elif absolute_path == "/vm_extra/get_pub_key":
	    response = get_pub_key()

	return response





def main():
    server_class = BaseHTTPServer.HTTPServer
    global HOST_NAME, PORT, VM_TYPES_FILEPATH,VMID
    VM_TYPES_FILEPATH = sys.argv[1]
    update_image_list()
    loadPMs()
    try:
        httpd = server_class((HOST_NAME,PORT), MasterHTTPServer)
        httpd.serve_forever()
    except Exception as e:
        print e


if __name__=="__main__":
    main()
