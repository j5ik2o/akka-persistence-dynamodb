FROM mesosphere/aws-cli

ADD *.json ./
ADD *.sh ./

ENTRYPOINT ["./create-table.sh"]