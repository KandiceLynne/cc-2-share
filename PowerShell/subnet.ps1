## Author - Kandice Hendricks
## Version 1.0
## Date 9/2/2021
## This is a very basic PowerShell Script that will do a compare of your input file
## to the already in use Cidr ranges in your subnets and will create one new subnet using a range 
## not currently used. This is much easier than using the !CIDR reference as that doesn't always function 
## easily.


## Replace the value for cidrs with your text file and path
$cidrs = Get-Content ./cidr.txt
## Replace the vpc value with the vpc id such as vpc-33444lasdfi
$vpc=<vpcid>
## No need to change anything in this value
$nacidrs=aws ec2 describe-subnets --filters "Name=vpc-id,Values=$vpc" --query "Subnets[*].{CIDR:CidrBlock}" --output text 
## Replace the az1 value with your option of az1 such as us-east-1a
$az1=<example us-east-1a>

ForEach ( $cidr in $cidrs )  {
        if ( $cidr -notin $nacidrs ) {
            try {
                aws ec2 create-subnet --vpc-id $vpc --availability-zone $az1 --cidr-block $cidr
                break 
            }
            catch {
               print "No available CIDR Ranges, clean up your subnets"
            }
        }
}
## This var needs to be defined again since you added a subnet in the block above
$nacidrs=aws ec2 describe-subnets --filters "Name=vpc-id,Values=$vpc" --query "Subnets[*].{CIDR:CidrBlock}" --output text 
##Replace below value with the value desired for az2
$az2='us-east-1b'
ForEach ( $cidr in $cidrs )  {
    if ( $cidr -notin $nacidrs ) {
        try {
            aws ec2 create-subnet --vpc-id $vpc --availability-zone $az2 --cidr-block $cidr
            break 
        }
        catch {
           print "No available CIDR Ranges, clean up your subnets"
        }
    }
}
