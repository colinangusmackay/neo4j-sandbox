Import-Module "$PSScriptRoot\Neo4j-Management.psd1"

$server = Get-Neo4jServer c:\neo4j
initialize-neo4jserver -Neo4jServer $server -ClearExistingDatabase