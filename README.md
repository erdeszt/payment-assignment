# Backend Technical Challenge

This challenge forms the basis for a conversation about how you approach development and balance tradeoffs. Imagine you are coming into an existing codebase, and are faced with a couple of issues and improvements.

**Time expectations:**

- We don't want to take too much of your time, so we don't expect you to use more than 2-4 hours on these challenges.
- Don’t worry too much about getting stuck on one of the assignments or running out of time. In that case, we can discuss potential solutions and challenges during the follow-up meeting.

**Solution expectations:**

- Make sure to read through the whole README to get an overview.
- You can change as much as you want, we will go through the choices together.
- If you get stuck, or you think there’s something that doesn’t make sense, please don’t hesitate to reach out!


**If you want to detail anything about your solution, include it here:**
<!-- START of your notes on the solution -->
- I haven't finished `3.b` but the same idea used for payments should work there as well.
- I think the two queries in `Payers.balance` could be merged into one, but I didn't spend time on it.
- I'm not happy about the complexity of `Payments.cover` but it's a fixed number of queries (so no `N+1` issue) and most of them should be "fast enough"
    - Thought about doing it by introducing a `Coverage` table with `(Invoices.Id, Payments.Id)` pairs, that would be very fast and simple to query
      but it looked very complex on the write side when I tried to implement it.
- There's a small bug in the `covers` method where it can skip over larger unpaid invoices and mark later, unpaid invoices that are within budget as covered.
  The fix should be relatively simple.

<!-- END of Notes -->

## Overview
- [Assignment: Payments, Invoices, and Balance](#assignment-payments-invoices-and-balance)
  - [Part 1: Failing tests](#part-1-failing-tests)
  - [Part 2: Inefficiencies](#part-2-inefficiencies)
  - [Part 3: Extend payment functionality](#part-3-extend-payment-functionality)
  - [Part 4: Extend payer functionality](#part-4-extend-payer-functionality)
- [Development](#development)
  - [Overview of Code](#overview-of-code)
  - [Database](#database)
- [Editor Setup](#editor-setup)
  - [VS Code](#vs-code)
  - [IntelliJ](#intellij)

## Assignment: Payments, Invoices, and Balance

The assignment consists of 4 parts, but first some context: Famly enables managers to create invoices that are sent to parents, as well as track payments of various amounts from the parents. A history of an account might look like this:


| **Date**   | **Action**                  |
|------------|-----------------------------|
| 2020-01-03 | Invoice 1 created for $350  |
| 2020-01-14 | Payment 1 received for $225 |
| 2020-01-29 | Invoice 2 created for $85   |
| 2020-02-08 | Payment 2 received for $210 |
| 2020-02-21 | Payment 3 received for $150 |


<p align="center">
    <i>Table 1: Account for Mr. Johnson (parent of Archie Johnson and Debbie Johnson):</i>
</p>

To get the balance for any given date, we consider all activity since the account was created and up to the date we’re checking for. In the above example, the balance on `2020-01-20` would be: `-$350 + $225 = -$125`.

### Part 1: Failing tests

An implementation of a balance calculation has already been added in the calculateBalance function in Payers.scala, along with a test in ChallengeTest.scala (`Payers should have the right balance after adding invoices and payments`). Unfortunately, the test is currently failing.

Figure out what causes the test failure, and update the implementation such that the test passes.

### Part 2: Inefficiencies

For accounts that have a lot of items, the current approach taken in the balance calculation implementation quickly becomes inefficient. Design a solution that would allow fetching the balance without fetching all items.

Focus on how to expand the database tables and data structure and how to efficiently save and fetch the data.

### Part 3: Extend payment functionality

Implement the following functionality without fetching all invoices and payments from the database. Depending on your solution, this might require you to add new tables or columns to the database.

**a)** For a single payment, list which invoices it covers.

Note that a payment may cover multiple invoices. In the example for Mr. Johnson above:

- Payment 1 is covering Invoice 1,
- Payment 2 is covering both Invoice 1 and Invoice 2, since Payment 1 does not cover the full amount of Invoice 1
- Payment 3 is not covering any invoice (yet), since all invoices have been paid in full by the time Payment 3 is received

**b)** For a single invoice, list which payments cover it.

Note that an invoice may be covered by multiple payments. In the example for Mr. Johnson above:

- Invoice 1 is covered by Payment 1 and Payment 2,
- Invoice 2 is covered by Payment 2

**c)** Add a few tests to the existing test suite showing that your implementations of steps a. and b. above are correct.

### Part 4: Extend payer functionality

In order to get an overview over unpaid amounts, managers would like to be able to see the outstanding balance for each payer in the system. Implement such balance listing for all payers and add an appropriate test to the test suite.

## Development

You will need the following tools installed locally:

- [Docker](https://www.docker.com/products/docker-desktop) for the MySQL database.
- [sbt](https://www.scala-sbt.org) to manage dependencies.
- Java Development Kit (JDK) 11 or later installed locally prior to importing the project.

To run the unit tests:

```bash
$ sbt test
```

### Overview of Code

This project implements a simple web-server in Scala using the [http4s](https://http4s.org/) and [doobie](https://tpolecat.github.io/doobie/) libraries.

The main entry point can be found in `src/Main/scala/challenge/Main.scala`.
Tests can be found in `src/test/scala/challenge/ChallengeTest.scala`.

The files most likely to be of interest are:
- `src/main/scala/challenge/Server.scala`: The definition of the web-server. Defines how requests are routed to services.
- `src/main/scala/challenge/{Invoices, Payers, Payments}.scala`: The business logic.
- `db/migrations.sql`: The database table definitions.

### Database

The project is set up fully support a MySQL database, but MySQL must be available locally. See the `db/migrations.sql` file for table definitions.

You can run the Docker command below, from inside the `challenge/` folder, to spin up a MySQL instance in Docker:

```bash
$ cd challenge
$ docker run \
   --name challenge-mysql \
   --env MYSQL_ROOT_PASSWORD=root \
   -p 3306:3306 \
   --mount type=bind,source=$(pwd)/db/migrations.sql,target=/docker-entrypoint-initdb.d/migrations.sql \
   --detach \
   mysql:5.7
```

## Editor Setup

Remember to make sure you have the Java Development Kit (JDK) 11 or later installed locally prior to importing the project.

If you don't have it already, we recommend installing the JDK via the [AdoptOpenJDK](https://adoptopenjdk.net/) project. If you are planning on using IntelliJ, you might be able to skip this step.

### VS Code

To import the project into VS Code:

1. Open the workspace (from the file `challenge.code-workspace`)
2. Install the recommended extension, `Scala (Metals)`
3. A pop-up should ask you if you want to import the SBT project - click Yes.
   If no pop-up appears, run the command `Metals: Import build`
4. Once the import finishes, run `Metals: Recompile workspace`

You may be prompted to install the bloop compilation server by the extension if
you do not already have it installed - click yes.

Once you have successfully compiled the project once, metals will automatically
incrementally compile any changes you make.

You can run the `Main` program and the `ChallengeTest` suite by going to the
"Run" left-side tab and selecting either of them.

For further help on importing, visit the [Metals installation guide](https://scalameta.org/metals/docs/editors/vscode.html#installation).

### IntelliJ

To import the project into IntelliJ:

1. Select `File > New > Project from Existing Sources...`
2. In the file browser, select the parent folder for the project (the folder containing the `build.sbt` file)
3. In the next modal, choose `sbt` from the `Import project from external model` options
4. Set up your JDK options as appropriate, it should be fine to leave everything as default.
