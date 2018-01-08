# Randomized Heavy Hitter Hierarchy Management

![N|Solid](https://cdn2.hubspot.net/hubfs/486579/lp/academy/sniffer.png)

## Get started
Download the Jar from https://www.dropbox.com/s/fuutdf698w6gk8c/RHHH_project-1.1.0-jar-with-dependencies.jar?dl=0
OVS rule definition:
1.	Open new Terminal
2.	insert the following command: “ovs-ofctl add-flow *virtual_switch* action=normal, output:*VM_port_number*”
For example: 
```# ovs-ofctl add-flow s1 action=normal, output:1 ```
Where ‘VM_port_number’ represent the supervisor’s VM that run the software configured income port for this specific use.
Will reroute the traffic from virtual switch s1 to port 8000 on vm
3.	Open new Terminal, go to command prompt and run the Jar by inserting the following command: 
.```# sudo path_to_jar/RHHH_project-1.1.0-jar-with-dependencies.jar *theta *epsilon 
*update_frequency ```
*see sector: command line parameters
4.	Choose from the list below the specified device to listen to (your network device)
5.	Go to your tmp directory, there will be a new sub folder called ‘rhhh’, in the path ```/tmp/rhhh``` the result will be saved, where the readable report will be the file ```display.html``` (open in any browser) and watch the result.

## Next step: read the results
After you lunch the RHHH SW, you will pop an html page on your default browser.
On the first sector, you can see the specific Current heavy hitters (see picture 1), where the IP prefixes shown on the left column and the number of hits at the right. The numbers that written at the file are the IP prefix of the domains that has discovered as Heavy hitters. Meaning that they in charge of, approximately, more traffic percentage then the threshold (of the whole traffic). 
You can see additional information above; the time past we started the run, total number of HH and the number of packages that we used in the analyses. 

 >***put here the picture
>Picture 1

## Command line parameters
The two arguments, that are not mandatory, are: epsilon, theta. 
Epsilon represent the size of the table where we keep the information about the traffic behavior of IP prefixes. For each level in the hierarchy (1 to 4) we hold one table. The bigger the epsilon, the smaller out error is. For example, for Epsilon=100 the table will contain at most 100 prefixes. 
Theta represent the threshold. This is the percentage of total traffic, which define a prefix as HH. For example, for theta=0.2 (20%) and traffic of 1M packets (after we filtered 1 packet out of 10 randomly) - a prefix will be considered HH if we sampled at least 200k packets* that sent by that prefix.
*this number is not accurate, there is dependent reduce according to the level in hierarchy and counters in the sub-prefixes. For further reading refer to the article. 
## Support 
For any issue feel free to contact us throw github
