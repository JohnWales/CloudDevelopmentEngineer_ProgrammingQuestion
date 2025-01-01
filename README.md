# CloudDevelopmentEngineer_ProgrammingQuestion

## Scenario
- A fleet of vehicles is operating in the Dublin area. Each vehicle is equipped with a telematics device that
acquires a set of tracking parameters once per second. When the device has accumulated 10 such sets,
it transmits them in a batch to the backend service.
- In order to save costs on mobile data usage, each 10 second batch is ZIP compressed and then base 64
encoded before being sent to the cloud.
- An example illustrating the structure of the data is shown below:
```
{
“vehicle_id”: “ba648eb1-3344-46e0-a46b-fc69ffe81614”,
“payload”:
“eJytzEEKgzAQheGrhNk2ljep2ppzdFe6kBpkEFPR7MS7OzdoF7N7Pzy+ncrafybJI0X32qnInLbSz4smBQRUCBV3T25ij
YjmCsBfoBPkHcmYpcg365s1tyWlQTcO735araF1N7QehlZnZzEMLTa0gqF1M7Tqf633cQL0y8kA”
}
```
- Once received in the cloud, the data isready to be processed.


## The Task
- You will be provided with a data file to be used as the invocation input for a Lambda function.
Given this input, create a Lambda function that answers the following questions in its execution output:
1. **How many vehicles are there in total?**
2. **Which vehicle drives the fastest on average?**
3. **Given the following table for determining a vehicle’s status:**

```
Status   Ignition   Speed
PARKED   0          0
IDLING   1          0
MOVING   1          1 +
```

- **Which vehicle has been parked the longest?**
- **Which vehicle has been idling the longest?**
- **Which vehicle has been moving the longest?**
