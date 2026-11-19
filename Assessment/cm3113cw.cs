// See https://aka.ms/new-console-template for more information
using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;


/* test settings */
int votingDuration = 1000; // voting duration in ms
int numberServers = 4;
int votesPerServerPerMS = 200;
int numberSubstitutes = 4;
Stopwatch stopwatch = new Stopwatch(); // Added stopwatch

/* Main program sequence */
Console.WriteLine("Starting Voting for " + (votingDuration/1000.0) + " seconds");
Match theMatch = new Match(numberServers, votesPerServerPerMS, numberSubstitutes, votingDuration);
Thread matchThread = new Thread(theMatch.SubstitutionTask);
theMatch.CreatePlayers();
Thread.Sleep(10);
theMatch.CreateServers();
Thread.Sleep(10);
Console.WriteLine("Match: " + theMatch);
stopwatch.Start(); // Start stopwatch to measure actual time for voting to complete
theMatch.StartServers();
matchThread.Start();  // uncomment this to include substitutions
Thread.Sleep(votingDuration);
theMatch.CloseVoting();
stopwatch.Stop(); // Stop stopwatch 
Console.WriteLine("Timer stopped: " + stopwatch.ElapsedMilliseconds + " ms"); // Display time to complete voting

theMatch.CheckData();
Thread.Sleep(1000);
theMatch.CheckData();


/* --------------------------------------------------------------------------*/
/* Match class encapsulates shared Player of the Match voting data           */
/* --------------------------------------------------------------------------*/
public class Match
{
    private int numberServers;
    private static long nextVoterID = 1000000000;
    private long totalVotes;

    private int votesPerServerPerMS;

    private int numberSubstitutes;

    private int votingDuration;

    private Random randomPlayerIndex = new Random();

    List<Player> players;
    Dictionary<Player, List<string>> playerVoters;
    Server[] servers;

    public Match(int nServers, int votes, int subs, int duration)
    {
        totalVotes = 0;
        numberServers = nServers; 
        votesPerServerPerMS = votes;
        numberSubstitutes = subs;
        votingDuration = duration;
        players = new List<Player>();
        playerVoters = new Dictionary<Player,List<string>>();
        servers = new Server[numberServers];
    }

    public void CreateServers()
    {
        for(int i = 0; i < numberServers; i++){
            servers[i] = new Server(this, votesPerServerPerMS, i); 
        }
    }

    public void StartServers()
    {
        foreach(Server s in servers){
            s.StartServer();
        }
    }

    public void CloseVoting()
    {
        foreach(Server s in servers){
            s.serverRunning = false;
        }
    }

    public string GetNextVoterID()
    {
        return "Voter" + (++nextVoterID); 
    }

    public void CreatePlayers()
    {
        for(int p = 1; p <= 11; p++)
        {
            players.Add(new Player("Team A Player " + p));
            players.Add(new Player("Team B Player " + p));
        }
        foreach(Player p in players)
        {
            playerVoters.Add(p, new List<string>());
        }  

    }

    public Player PickPlayer()
    {
        return players[randomPlayerIndex.Next(players.Count)];
    }

    public void ProcessVote(Vote v)
    {
        Player p = v.player;
        string s = v.voterID;
        totalVotes++;
        p.AddVote();
        playerVoters[p].Add(s);
    }

    public void SubstitutionTask()
    {   // simulates some players being added to the player list
        int gaps = numberSubstitutes + 1;
        for(int i = 0; i < gaps; i++)
        {
            Thread.Sleep(votingDuration / numberSubstitutes);
            Console.WriteLine("Theres been a substitution");
            Player p = new Player("Player " + (players.Count +1));
            players.Add(p);
            playerVoters.Add(p, new List<string>());
        } 
    }

    public void CheckData()
    {
        Console.WriteLine("Total votes counted by Match = " + totalVotes);

        long totalPlayerVotes = 0;
        foreach(Player p in players) {totalPlayerVotes += p.votes;}
        Console.WriteLine("Total votes counted by Players = " + totalPlayerVotes);

        long totalServerVotes = 0;
        foreach(Server s in servers) {totalServerVotes += s.GetVotesMade();}
        Console.WriteLine("Total votes sent by Servers = " + totalServerVotes);

        Console.WriteLine("Voting counts of individual players:");
        int numberPlayersInconsistent = 0;
        foreach(Player p in players)
        {
            int difference = p.votes - playerVoters[p].Count;
            if(difference != 0) {
                Console.WriteLine(p.playerName + " vote count inconsistent by " + difference);
                numberPlayersInconsistent++;
            }
        }
        Console.WriteLine(numberPlayersInconsistent + " players had inconsistent counts");
    }

    public override string ToString()
    {
        return "Number servers = " + numberServers + " Number players = " + players.Count + " votes per ms = "  + votesPerServerPerMS;
    }


}

/* --------------------------------------------------------------------------*/
/* Server is a class handling Voter connections */
/* --------------------------------------------------------------------------*/
public class Server
{
   int serverNumber;
   int votesPerServerPerMS;
   BlockingCollection<Vote> voteBuffer; // changed queue to blocking collection
   public Match match{get;}

   Thread connectionHandlerThread, voteProcessorThread;

   private long votesMade;

   public bool serverRunning{get; set;}

   public Server(Match m, int v, int n)
    {
        serverNumber = n;
        votesPerServerPerMS = v;
        match = m;
        votesMade = 0;
        voteBuffer = new BlockingCollection<Vote>();
        connectionHandlerThread = new Thread(ServerVoterConnectionTask);
        voteProcessorThread = new Thread(VoteProcessorTask);
    }

    public void StartServer()
    {
        serverRunning = true;
        connectionHandlerThread.Start();  
        voteProcessorThread.Start();       
    }

   public void SubmitVote(Vote v)
    { 
        // match.ProcessVote(v);
        voteBuffer.Add(v);  // Changed enqueue to add to blockingcollection
        Interlocked.Increment(ref votesMade); // thread safe implementation of an increment
    }

    public void ServerVoterConnectionTask() // Thread pool implemented here
    {
        while (serverRunning)
        {        
            Task[] taskList = new Task[votesPerServerPerMS + 1]; // array of tasks


            for(int n = 0; n <= votesPerServerPerMS; n++)
            {
                VoteHandler voteHandler = new VoteHandler(this);
                
                taskList[n] = Task.Run(voteHandler.VoterTask); // Assign work to task where threadpool will use it.
            } 

            Task.WaitAll(taskList); // Wait for tasks to complete before continuing

            Thread.Sleep(1);
            // Console.WriteLine(votesPerServerPerMS + " votes added via Server " + this); (this was here before any changes)
        }
        voteBuffer.CompleteAdding(); //  no more votes added to buffer
        
    } 

    public void VoteProcessorTask()
    {
        while (!voteBuffer.IsCompleted) // while buffer is not empty and open to new additions
        {
            if(voteBuffer.TryTake(out Vote v))
            {
                match.ProcessVote(v); // TryTake returns true if able to remove an item or blocks until item is available
            } 
        }
    }

    public override string ToString()
    {
        return "Server " + serverNumber + " made " + this.votesMade + " votes so far";
    }

    public long GetVotesMade() { return this.votesMade; } 

}



/* --------------------------------------------------------------------------*/
/* VoteHandler is a class handling Voter choices */
/* --------------------------------------------------------------------------*/
public class VoteHandler
{
    private Server server;

    public VoteHandler(Server s)
    {
        server = s;
    }

    public void VoterTask()
    {  
        Player p = server.match.PickPlayer();
        string voterID = server.match.GetNextVoterID();
        Vote v = new Vote(voterID, p);
        server.SubmitVote(v);
    }

    
}



/* --------------------------------------------------------------------------*/
/* Vote is a simple class encapulating a unique voterID                      */
/* and a reference to the Player voted for                                   */
/* --------------------------------------------------------------------------*/
public class Vote
{
    public string voterID;
    public Player player{get; set;}

    public Vote(string id, Player p)
    {
        voterID = id;
        player = p;
    }    
}



/* --------------------------------------------------------------------------*/
/* Player is a simple class encapulating a player name 
 * and a count of the number of times they have been voted for */
 /* --------------------------------------------------------------------------*/
 public class Player
{
    public string playerName{get; set;}
    public int votes {get; set;}
    public Player(string name)
    {
        votes = 0;
        playerName = name;
    }

    public void AddVote()
    {
        votes++;
    }

}



