export TF_VAR_MONGODB_ATLAS_PROJECT_ID=<<YOURVALUE>>
export TF_VAR_MONGODB_ATLAS_INSTANCE_NAME=spitest
export TF_VAR_MONGODB_ATLAS_PUBLIC_KEY=<<YOURVALUE>>
export TF_VAR_MONGODB_ATLAS_PRIVATE_KEY=<<YOURVALUE>>
export TF_VAR_OLD_PROCESSOR=<<YOURVALUE>>
export TF_VAR_NEW_PROCESSOR=<<YOURVALUE>>
export TF_VAR_RESUME_TOKEN=<<YOURVALUE>>

ln -s scripts/createprocessor.tf createprocessor.tf
terraform init
terraform apply

rm createprocessor.tf
ln -s scripts/stoptoken.tf stoptoken.tf
terraform apply
terraform output -json > ./scripts/resumeToken.txt

rm stoptoken.tf
ln -s scripts/delete.tf delete.tf
terraform apply

rm delete.tf
ln -s scripts/updateprocessor.tf updateprocessor.tf
terraform apply

rm updateprocessor.tf
