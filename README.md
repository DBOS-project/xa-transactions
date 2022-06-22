# xa-transactions
Repo for exploring XA transactions between DBMSs.

First, install dependencies:
```bash
sudo apt install openjdk-11-jdk maven libatomic1
```

Assume you put this repo in the same folder as [apiary](https://github.com/DBOS-project/apiary.git). You could directly try to compile and test.
Run the following commands under the root directory of this project:

```bash
mvn clean
mvn test
```
The first command will clean up previous build and install Apiary as a dependency. Every time you update the Apiary repo, you will need to re-install it through `mvn clean` in this repo. 