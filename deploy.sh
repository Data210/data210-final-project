docker run --env-file env.list --name datapipeline -e filename="${1:""}" data210pipelineproduction.azurecr.io/sparta:pipeline-production
# docker run --env-file env.list -e filename=Jan2020 data210pipelineproduction.azurecr.io/sparta:pipeline-production
az container delete --name datapipeline --resource-group container-instances-rg -y