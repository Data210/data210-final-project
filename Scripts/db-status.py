import docker
import shutil

class DockerDebug:
    def __init__(self):
        if shutil.which('docker') is not None:
            print('Docker is installed.')
        else:
            print('Docker is not installed.')
            print('Please Docker install from: https://docs.docker.com/engine/install/')
            return
        
    def check_runnning(self):
        try:
            self.client = docker.from_env()
            print("Docker is running")
            return True
        except:
            print("Docker is not running. Please open the docker application.")
            return False

    def check_container(self, container_name):
        try:
            container = self.client.containers.get(container_name)
            if container.status == "running":
                print(f"The {container_name} container is running!")
            else:
                print(f"The {container_name} container is not running.")
        except:
            print(f"The {container_name} container does not exist.")
            self.client.containers.run(
                'mcr.microsoft.com/azure-sql-edge',
                detach=True,
                name=container_name,
                ports={'1433/tcp': 1433},
                environment={
                    'ACCEPT_EULA': 'Y',
                    'MSSQL_SA_PASSWORD': 'yourStrong(!)Password'
                }
            )
            print("Container created and running")
 

debug = DockerDebug()
result = debug.check_runnning()
if result:
    debug.check_container('azuresqledge')