using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Serilog;

namespace MovieDatabaseWorker
{
    public class Program
    {
        static void Main(string[] args)
        {
#if DEBUG
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Debug()
                .WriteTo.ColoredConsole()
                .WriteTo.File("Logs\\Log.txt", rollingInterval: RollingInterval.Day)
                .CreateLogger();
#else
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information();
                .WriteTo.ColoredConsole()
                .WriteTo.File("Logs\\Log.txt", rollingInterval: RollingInterval.Day)
                .CreateLogger();
#endif

            Log.Information("Program started");
            _eh.ResetConsecutvieErrorCount();

            Worker worker = new Worker();
            bool successful = false;

            Program.tempmoviestatus = null;

            do
            {
                try
                {
                    Program.tempmoviestatus = Program.BLL_TempMovieStatus.SelectByID_model(1);
                    successful = (Program.tempmoviestatus != null);
                    _eh.ResetConsecutvieErrorCount();
                    Log.Information("TempMovieStatus record loaded.");
                }
                catch (Exception ex)
                {
                    Program.tempmoviestatus = null;
                    successful = false;
                    _eh.IncreaseConsecutvieErrorCount();
                    Log.Error("TempMovieStatus record load failed.", ex);
                    Log.Error("Consecutive error cound = {ConsecutiveErrorCount}", _eh.ConsecutiveErrorCount);
                }
            } while (!successful);

            if (Program.tempmoviestatus != null)
            {
                worker.CheckStartingState();

                while (Program.KeepRunning == true)
                {
                    worker.DoWork();
                }
            }
            else
            {
                _eh.IncreaseConsecutvieErrorCount();
                Log.Error("Unable to load TempMovieStatus object.");
                Log.Error("Consecutive error cound = {ConsecutiveErrorCount}", _eh.ConsecutiveErrorCount);
                Log.Fatal("Unable to load TempMovieStatus object. Cannot proceed.");
            }
        }

        // Global variables
        public static MDBW_ErrorHandler _eh = new MDBW_ErrorHandler();

        public static DA.Models.MovieDatabase.TempMovieStatus tempmoviestatus = null;
        public static DA.Models.MovieDatabase.TempMovie tempmovie = null;
        public static DA.Models.MovieDatabase.TempPerson tempperson = null;
        public static DA.Models.MovieDatabase.Movie movie = null;
        public static DA.Models.MovieDatabase.Person person = null;
        public static DA.Models.MovieDatabase.MovieQueue moviequeue = null;
        public static DA.Models.MovieDatabase.TVEpisodeQueue tvepisodequeue = null;

        public static String TempMovieSource = String.Empty;
        public static String DateOfError = String.Empty;
        public static String ErrorDescription = String.Empty;
        public static bool KeepRunning = true;

        public static API_IMDB.Classes.TempPersonResponse temppersonresponse = null;
        public static API_IMDB.Classes.TempMovieResponse tempmovieresponse = null;

        public static int ErrorCount = 0;
        public static bool IsStuck = false;

        // BLLs
        public static DA.BLL.MovieDatabase.APIRequests BLL_APIRequests = new DA.BLL.MovieDatabase.APIRequests(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempMovieStatus BLL_TempMovieStatus = new DA.BLL.MovieDatabase.TempMovieStatus(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.name_basics BLL_name_basics = new DA.BLL.MovieDatabase.name_basics(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.MovieQueue BLL_MovieQueue = new DA.BLL.MovieDatabase.MovieQueue(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempMovie BLL_TempMovie = new DA.BLL.MovieDatabase.TempMovie(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.Movies BLL_Movies = new DA.BLL.MovieDatabase.Movies(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.People BLL_People = new DA.BLL.MovieDatabase.People(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TVEpisodeQueue BLL_TVEpisodeQueue = new DA.BLL.MovieDatabase.TVEpisodeQueue(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TVEpisodeQueue_Errors BLL_TVEpisodeQueue_Errors = new DA.BLL.MovieDatabase.TVEpisodeQueue_Errors(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.MovieQueue_Errors BLL_MovieQueue_Errors = new DA.BLL.MovieDatabase.MovieQueue_Errors(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.AKAs BLL_AKAs = new DA.BLL.MovieDatabase.AKAs(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.BusinessData BLL_BusinessData = new DA.BLL.MovieDatabase.BusinessData(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.CastMembers BLL_CastMembers = new DA.BLL.MovieDatabase.CastMembers(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.Countries BLL_Countries = new DA.BLL.MovieDatabase.Countries(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.Directors BLL_Directors = new DA.BLL.MovieDatabase.Directors(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.FilmingLocations BLL_FilmingLocations = new DA.BLL.MovieDatabase.FilmingLocations(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.Genres BLL_Genres = new DA.BLL.MovieDatabase.Genres(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.Goofs BLL_Goofs = new DA.BLL.MovieDatabase.Goofs(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.KeywordList BLL_KeywordList = new DA.BLL.MovieDatabase.KeywordList(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.Keywords BLL_Keywords = new DA.BLL.MovieDatabase.Keywords(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.Languages BLL_Languages = new DA.BLL.MovieDatabase.Languages(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.Lines BLL_Lines = new DA.BLL.MovieDatabase.Lines(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.MovieQuotes BLL_MovieQuotes = new DA.BLL.MovieDatabase.MovieQuotes(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.MovieTrivia BLL_MovieTrivia = new DA.BLL.MovieDatabase.MovieTrivia(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.Nicknames BLL_Nicknames = new DA.BLL.MovieDatabase.Nicknames(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.PersonQuotes BLL_PersonQuotes = new DA.BLL.MovieDatabase.PersonQuotes(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.PersonTrivia BLL_PersonTrivia = new DA.BLL.MovieDatabase.PersonTrivia(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.SimilarMovies BLL_SimilarMovies = new DA.BLL.MovieDatabase.SimilarMovies(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.SimpleTVEpisodes BLL_SimpleTVEpisodes = new DA.BLL.MovieDatabase.SimpleTVEpisodes(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TechnicalData BLL_TechnicalData = new DA.BLL.MovieDatabase.TechnicalData(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempAKAs BLL_TempAKAs = new DA.BLL.MovieDatabase.TempAKAs(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempBusinessData BLL_TempBusinessData = new DA.BLL.MovieDatabase.TempBusinessData(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempCastMembers BLL_TempCastMembers = new DA.BLL.MovieDatabase.TempCastMembers(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempCountries BLL_TempCountries = new DA.BLL.MovieDatabase.TempCountries(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempDirectors BLL_TempDirectors = new DA.BLL.MovieDatabase.TempDirectors(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempFilmingLocations BLL_TempFilmingLocations = new DA.BLL.MovieDatabase.TempFilmingLocations(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempGenres BLL_TempGenres = new DA.BLL.MovieDatabase.TempGenres(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempGoofs BLL_TempGoofs = new DA.BLL.MovieDatabase.TempGoofs(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempKeywords BLL_TempKeywords = new DA.BLL.MovieDatabase.TempKeywords(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempLanguages BLL_TempLanguages = new DA.BLL.MovieDatabase.TempLanguages(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempLines BLL_TempLines = new DA.BLL.MovieDatabase.TempLines(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempMovieQuotes BLL_TempMovieQuotes = new DA.BLL.MovieDatabase.TempMovieQuotes(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempMovieTrivia BLL_TempMovieTrivia = new DA.BLL.MovieDatabase.TempMovieTrivia(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempNicknames BLL_TempNicknames = new DA.BLL.MovieDatabase.TempNicknames(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempPersonQuotes BLL_TempPersonQuotes = new DA.BLL.MovieDatabase.TempPersonQuotes(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempPersonTrivia BLL_TempPersonTrivia = new DA.BLL.MovieDatabase.TempPersonTrivia(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempSimilarMovies BLL_TempSimilarMovies = new DA.BLL.MovieDatabase.TempSimilarMovies(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempSimpleTVEpisodes BLL_TempSimpleTVEpisodes = new DA.BLL.MovieDatabase.TempSimpleTVEpisodes(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempTechnicalData BLL_TempTechnicalData = new DA.BLL.MovieDatabase.TempTechnicalData(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.TempWriters BLL_TempWriters = new DA.BLL.MovieDatabase.TempWriters(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.Writers BLL_Writers = new DA.BLL.MovieDatabase.Writers(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.PeoplePhotos BLL_PeoplePhotos = new DA.BLL.MovieDatabase.PeoplePhotos(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.MoviePosters BLL_MoviePosters = new DA.BLL.MovieDatabase.MoviePosters(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
        public static DA.BLL.MovieDatabase.WorkerConfiguration BLL_WorkerConfiguration = new DA.BLL.MovieDatabase.WorkerConfiguration(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);

    }
}
