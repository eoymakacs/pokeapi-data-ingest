# PokeDB

The [PokeAPI](https://pokeapi.co/) is a public, unauthenticated REST API which allows you to query stats and information about most generations of Pokemon. This challenge will comprise 2 parts: ingesting the PokeAPI data into a SQLite database, and writing queries against your database to answer our questions. 

## Guidelines:
*  For this project we will only consider the first generation of Pokemon, which have IDs in the range [1,151] inclusive.
*  We will provide an empty Github repo to submit your project against. Please put your work in a pull request onto the main branch
*  You can install whichever additional dependencies you'd like, but make sure to reflect them in a requirements.txt file. If you needed other configuration, please reflect how to reproduce your environment setup in a README.
*  If you would like to use window functions, be sure to use sqlite >= 3.25.0.
*  Feel free to have your answers to these queries in a SQL file, or in an executable format in a Python script that runs it against your database. 

## Queries to answer:

1. Find the count of all distinct counts of Pokemon types (ex: water:14, grass/poison: 5)

2. All Pokemon start with base/initial stat values. These stats are: hp, speed, attack, defense, special attack, special defense. For each type(s), rank order each Pokemon based off the sum of their stats.

3. A Pokemon is considered `tanky` if hp or defense is their top stat. Write a query that labels each Pokemon as 'tanky' or 'not tanky'.

4. Which Pokemon type(s) have the most tanky Pokemon?
