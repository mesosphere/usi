# Contributors Guide

@@@ index

* [Code Culture](code-culture.md)

@@@ 

Make contributions with GitHub [pull requests](https://github.com/mesosphere/usi/pulls).

### Bugs / Feature Requests
Think you’ve found a bug? Want to see a new feature? Please open a case in our issue management tool, JIRA:
* Login to the [DC/OS USI Public JIRA](https://jira.mesosphere.com/projects/DCOS_OSS/issues).  You will need a Github or Google account to use this service.
* Navigate to the **DC/OS OSS USI project**.
* Click **Create Issue** - Please provide as much information as possible about the issue type and how to reproduce it.
* Github Issues for this project have been disabled.
* Bug reports in JIRA for the Mesosphere USI project are public.

If you would like to propose a new feature or a significant change, first open an issue (instructions above) to discuss it.  Project shepherds will provide feedback on the proposed change in terms of feasibility, design, and implementation guidance.  While all proposals are welcome, those that conflict with our roadmap or design principles may not be accepted.

### Creating Pull Requests
Create pull requests against the [`master`](https://github.com/mesosphere/usi/tree/master) branch. Be sure to include unit tests and integration tests, as well as updates to the documentation, default scheduler, and reference framework if necessary.

See the (testing documentation)[TESTING.md] for instructions on running static code checks and system integration tests.

Simple pull requests (e.g., to fix documentation or comments) are welcome without an issue, but any substantial changes require an issue and discussion ahead of time.

### Merge Approvals
Pull requests are reviewed and tested prior to merging.  Often shepherds will ask contributors to make changes based on our style guidelines, design principles, or project goals.  An initial response is typically provided in less than one week.

### Communication
Most communication is done primarily over Slack. Join us via http://chat.dcos.io in the __#usi__ and __#usi-dev__ channels, for users and developers respectively.

To build a healthy, respectful community, we follow [Github’s community guidelines](https://help.github.com/articles/github-community-guidelines/#building-a-strong-community).

### Licensing
Mesosphere USI is licensed under the [Apache License, Version 2.0](../../../../LICENSE). Any code you submit must be under that license.

For significant changes, we may ask you to sign a [Contributor License Agreement](http://en.wikipedia.org/wiki/Contributor_License_Agreement).
