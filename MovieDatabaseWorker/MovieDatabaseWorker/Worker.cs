using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;
using System.Timers;
using System.Net;
using System.Drawing;
using System.Threading;
using System.Data.SqlClient;

namespace MovieDatabaseWorker
{
    public class Worker
    {

        #region Primary Control

        /// <summary>
        /// Main function that simply calls the ExecuteStages method.
        /// </summary>
        public void DoWork()
        {
            ExecuteStages();
        }

        /// <summary>
        /// This method executes the proper stage based on the value stored in the Stage field of the
        /// TempMovieStatus table.
        /// </summary>
        public void ExecuteStages()
        {
            switch (Program.tempmoviestatus.Stage)
            {
                case 0:
                    Stage00_GetNextTempMovie();
                    break;
                case 1:
                    Stage01_DetermineAdd();
                    break;
                case 2:
                    Stage02_SaveTempMovieToTempTables();
                    break;
                case 3:
                    Stage03_AddCastMembersToPeopleTable();
                    break;
                case 4:
                    Stage04_AddDirectorsToPeopleTable();
                    break;
                case 5:
                    Stage05_AddWritersToPeopleTable();
                    break;
                case 6:
                    Stage06_AddMovieToMoviesTableAndUpdateMovieIDs();
                    break;
                case 7:
                    Stage07_AddFinalTables();
                    break;
                case 8:
                    Stage08_FinalizeAndReset();
                    break;
            }
        }

        #endregion

        #region Stage00

        /// <summary>
        /// Get the next temp movie from either the MovieQueue or TVEpisodeQueue.
        /// </summary>
        public static void Stage00_GetNextTempMovie()
        {
            Console.Title = "Stage 0";
            Console.WriteLine(DateTime.Now.ToString() + " - Getting next temp movie.");
            Program.tempmovie = null;
            bool QuerySuccess = false;

            // Truncate all temporary tables.
            QuerySuccess = false;
            do
            {
                try
                {
                    QuerySuccess = Program.BLL_TempMovieStatus.TruncateAll();
                }
                catch (Exception ex)
                {
                    QuerySuccess = false;
                }
            } while (!QuerySuccess);
            Console.WriteLine(DateTime.Now.ToString() + " - All temp tables truncated.");

            DataTable dt = null;

            QuerySuccess = false;
            do
            {
                try
                {
                    dt = Program.BLL_TVEpisodeQueue.SelectNext_datatable();
                    QuerySuccess = (dt != null);
                }
                catch
                {
                    QuerySuccess = false;
                }
            } while (!QuerySuccess);

            if (dt.Rows.Count > 0)
            {
                Console.WriteLine(DateTime.Now.ToString() + " - TVEpisodeQueue selected.  Loading next movie.");

                // There is a record in the TVEpisodeQueue.  Make that
                // record the next TempMovie.
                Program.tvepisodequeue = null;
                do
                {
                    try
                    {
                        Program.tvepisodequeue = Program.BLL_TVEpisodeQueue.SelectNext_model();
                    }
                    catch
                    {
                        Program.tvepisodequeue = null;
                    }
                } while (Program.tvepisodequeue == null);

                Program.TempMovieSource = "TVEpisodeQueue";

                Program.tempmovie = null;
                try
                {
                    if (!Program.tvepisodequeue.MovieJSON.Equals(""))
                    {
                        Program.tempmovie = API_IMDB.IMDB.GetMovieByMovieJSON(Program.tvepisodequeue.MovieJSON);
                    }
                    else
                    {
                        API_IMDB.Classes.TempMovieResponse tempmovieresponse = API_IMDB.IMDB.GetMovieByIMDBID(Program.tvepisodequeue.EpisodeIMDBID, Connections.ConnectionStrings.myApiFilmsToken);
                        if (tempmovieresponse.Success)
                        {
                            Program.tempmovie = tempmovieresponse.oTempMovie;
                        }
                        else
                        {
                            Program.tempmovie = null;
                        }
                    }
                }
                catch
                {
                    Program.tempmovie = null;
                }

                if (Program.tempmovie != null)
                {
                    Program.tempmoviestatus.Stage = 1;
                    Program.tempmoviestatus.MovieSource = Program.TempMovieSource;
                    UpdateTempMovieStatus(Program.tempmoviestatus);
                }
            }
            else
            {
                Console.WriteLine(DateTime.Now.ToString() + " - MovieQueue selected.  Loading next movie.");

                // There is NOT a record in the TVEpisodeQueue.  Make
                // the next record in the MovieQueue the next TempMovie.

                Program.moviequeue = null;
                do
                {
                    try
                    {
                        Program.moviequeue = Program.BLL_MovieQueue.SelectNext_model();
                    }
                    catch
                    {
                        Program.moviequeue = null;
                    }
                } while (Program.moviequeue == null);

                Program.TempMovieSource = "MovieQueue";

                Program.tempmovie = null;

                // Make sure the MovieJSON member is not null or blank.  If it is, load JSON via API
                // and save it to the database.
                if ((Program.moviequeue.MovieJSON == null) || (Program.moviequeue.MovieJSON.Equals("")))
                {
                    API_IMDB.Classes.TempMovieJSONResponse resp;
                    QuerySuccess = false;
                    do
                    {
                        try
                        {
                            resp = API_IMDB.IMDB.GetMovieJSONByIMDBID(Program.moviequeue.IMDBID, Connections.ConnectionStrings.myApiFilmsToken);
                            QuerySuccess = resp.Success;
                        }
                        catch (Exception ex)
                        {
                            QuerySuccess = false;
                            resp = null;
                        }
                        Thread.Sleep(30000);
                    } while (!QuerySuccess);

                    Program.moviequeue.MovieJSON = resp.MovieJSON;

                    QuerySuccess = false;
                    do
                    {
                        Program.BLL_MovieQueue.Update(ref Program.moviequeue);
                        Program.moviequeue = Program.BLL_MovieQueue.SelectByQueueID_model(Program.moviequeue.QueueID);
                        QuerySuccess = Program.moviequeue.MovieJSON.Equals(resp.MovieJSON);
                    } while (!QuerySuccess);
                }

                // MovieJSON will be set at this point.
                // Get the Type from the MovieJSON
                String movietype = String.Empty;
                movietype = API_IMDB.IMDB.GetTypeFromJSON(Program.moviequeue.MovieJSON);

                bool proceed = VerifySupportedType(movietype);

                if (!proceed)
                {
                    AddToMovieQueueErrors(Program.moviequeue, movietype, "Unsupported type");
                    DeleteFromMovieQueue(Program.moviequeue.IMDBID);

                    Program.tempmoviestatus.Stage = 0;
                    Program.tempmoviestatus.MovieSource = "";
                    UpdateTempMovieStatus(Program.tempmoviestatus);
                }
                else
                {
                    do
                    {
                        try
                        {
                            Program.tempmovie = API_IMDB.IMDB.GetMovieByMovieJSON(Program.moviequeue.MovieJSON);
                        }
                        catch (Exception ex)
                        {
                            Program.tempmovie = null;
                        }
                    } while (Program.tempmovie == null);

                    Program.tempmoviestatus.Stage = 1;
                    Program.tempmoviestatus.MovieSource = Program.TempMovieSource;
                    UpdateTempMovieStatus(Program.tempmoviestatus);
                }

            }

            Console.WriteLine(DateTime.Now.ToString() + " - Temp Movie loaded.");
        }

        /// <summary>
        /// This subroutine determines if the movie is a supported type.  If so, it returns true.  If not,
        /// it returns false.
        /// Supported types: Movie, TV Movie, TV Series, TV Episode
        /// </summary>
        /// <param name="movietype">The movie type parsed from the MovieJSON member of the moviequeue object.</param>
        public static bool VerifySupportedType(String movietype)
        {
            bool supported = ((movietype.Equals("Movie")) || (movietype.Equals("TV Movie")) || (movietype.Equals("TV Series")) || (movietype.Equals("TV Episode")));
            return supported;
        }

        #endregion

        #region Stage01

        /// <summary>
        /// This subroutine continues the validation.  Supported type was verified in Stage 0.
        /// If the IMDB is already in the Movies table, do not add.  Delete from queue.
        /// If the type is TV Episode and the SeriesIMDB is not in the Movies table, do not add.  Move to errors.
        /// </summary>
        public static void Stage01_DetermineAdd()
        {
            Console.Title = "Stage 1";
            Console.WriteLine(DateTime.Now.ToString() + " - Determining addition conditions.");
            bool add = true;
            Program.DateOfError = String.Empty;
            Program.ErrorDescription = String.Empty;
            bool isMovieInDatabase = false;

            // If Program.tempmovie is null at this point, there was an error in Stage 0 loading the
            // tempmovie.  Reset to Stage 0 with Console message and try again.
            if (Program.tempmovie == null)
            {
                Console.WriteLine(DateTime.Now.ToString() + " - Program.tempmovie is null.  There was an error in Stage 0, likely when loading the tempmovie.  Resetting to Stage 0 to try again.");
                Program.tempmoviestatus.Stage = 0;
                Program.tempmoviestatus.MovieSource = "";
                UpdateTempMovieStatus(Program.tempmoviestatus);
                add = false;
            }
            else
            {
                // For the next add determination, check to see if the movie IMDBID is already in the
                // database.  If so, remove it from the MovieQueue, reset stage to 0 and start with the next movie.
                isMovieInDatabase = Program.BLL_Movies.Contains(Program.tempmovie.IMDBID);
                if (isMovieInDatabase)
                {
                    if (Program.TempMovieSource.Equals("MovieQueue"))
                    {
                        Console.WriteLine(DateTime.Now.ToString() + " - The movie was already in the database.  Deleting from Movie Queue.");
                        DeleteFromMovieQueue(Program.tempmovie.IMDBID);
                    }
                    else if (Program.TempMovieSource.Equals("TVEpisodeQueue"))
                    {
                        Console.WriteLine(DateTime.Now.ToString() + " - The episode was already in the database.  Deleting from TV Episode Queue.");
                        DeleteFromTVEpisodeQueue(Program.tempmovie.IMDBID);
                    }

                    Program.tempmoviestatus.Stage = 0;
                    Program.tempmoviestatus.MovieSource = "";
                    UpdateTempMovieStatus(Program.tempmoviestatus);
                    add = false;
                }
                else
                {
                    // If the movie is a TV Episode and the TV Series has not yet been added to the
                    // database, move to errors and delete from Movie Queue.
                    if (Program.tempmovie.Type.Equals("TV Episode"))
                    {
                        // Check to make sure the series IMDB exists in the Movies table.  If not, move to errors.
                        try
                        {
                            isMovieInDatabase = Program.BLL_Movies.Contains(Program.tempmovie.SeriesIMDBID);
                        }
                        catch (Exception ex)
                        {
                            // There was an error, likely setting tempmovie.  Reset to stage 0 and repeat.
                            // Reset stage to 0.
                            Program.tempmoviestatus.Stage = 0;
                            Program.tempmoviestatus.MovieSource = "";

                            UpdateTempMovieStatus(Program.tempmoviestatus);
                        }

                        if (!isMovieInDatabase)
                        {
                            // The series is not in the movie table yet.
                            // Move this to the TVEpisodeQueue_Errors table.
                            AddToTVEpisodeQueueErrors("The series for this episode has not yet been added.");

                            // Now, delete this record from the MovieQueue
                            DeleteFromMovieQueue(Program.tempmovie.IMDBID);

                            Program.tempmoviestatus.Stage = 0;
                            Program.tempmoviestatus.MovieSource = "";

                            UpdateTempMovieStatus(Program.tempmoviestatus);
                            add = false;
                        }
                        else
                        {
                            add = true;
                        }
                    }
                    else
                    {
                        add = true;
                    }
                }
            }

            if (add)
            {
                Program.tempmoviestatus.Stage = 2;
                UpdateTempMovieStatus(Program.tempmoviestatus);
            }

            Console.WriteLine(DateTime.Now.ToString() + " - Addition conditions determined.");
        }

        #endregion

        #region Stage02

        /// <summary>
        /// Save the tempmovie structure to the appropriate temp
        /// database tables.
        /// </summary>
        public static void Stage02_SaveTempMovieToTempTables()
        {
            Console.Title = "Stage 2";
            Console.WriteLine(DateTime.Now.ToString() + " - Saving temp movie to temp tables started...");

            bool QuerySuccess = false;
            long id = -1;

            // TempMovie
            id = Stage02_SaveTempMovie();
            Program.tempmovie.ID = id;
            Console.WriteLine(DateTime.Now.ToString() + " - TempMovie saved.");

            // TempBusinessData
            Stage02_SaveTempBusinessData(id);
            Console.WriteLine(DateTime.Now.ToString() + " - TempBusinessData saved.");

            // TempTechnicalData
            Stage02_SaveTempTechnialData(id);
            Console.WriteLine(DateTime.Now.ToString() + " - TempTechnicalData saved.");

            // TempCastMembers
            Stage02_SaveTempCastMembers();
            Console.WriteLine(DateTime.Now.ToString() + " - TempCastMembers saved.");

            // TempDirectors
            Stage02_SaveTempDirectors();
            Console.WriteLine(DateTime.Now.ToString() + " - TempDirectors saved.");

            // TempWriters
            Stage02_SaveTempWriters();
            Console.WriteLine(DateTime.Now.ToString() + " - TempWriters saved.");

            // TempSimpleTVEpisodes
            Stage02_SaveTempSimpleTVEpisodes();
            Console.WriteLine(DateTime.Now.ToString() + " - TempSimpleTVEpisodes saved.");

            // TempCountries
            Stage02_SaveTempCountries();
            Console.WriteLine(DateTime.Now.ToString() + " - TempCountries saved.");

            // TempFilmingLocations
            Stage02_SaveTempFilmingLocations();
            Console.WriteLine(DateTime.Now.ToString() + " - TempFilmingLocations saved.");

            // TempGenres
            Stage02_SaveTempGenres();
            Console.WriteLine(DateTime.Now.ToString() + " - TempGenres saved.");

            // TempLanguages
            Stage02_SaveTempLanguages();
            Console.WriteLine(DateTime.Now.ToString() + " - TempLanguages saved.");

            // TempMovieTrivia
            Stage02_SaveTempMovieTrivia();
            Console.WriteLine(DateTime.Now.ToString() + " - TempMovieTrivia saved.");

            // TempAKA
            Stage02_SaveTempAKA();
            Console.WriteLine(DateTime.Now.ToString() + " - TempAKAs saved.");

            // TempSimilarMovie
            Stage02_SaveTempSimilarMovie();
            Console.WriteLine(DateTime.Now.ToString() + " - TempSimilarMovies saved.");

            // TempGoofs
            Stage02_SaveTempGoofs();
            Console.WriteLine(DateTime.Now.ToString() + " - TempGoofs saved.");

            // TempKeywords
            Stage02_SaveTempKeywords();
            Console.WriteLine(DateTime.Now.ToString() + " - TempKeywords saved.");

            // TempMovieQuotes
            Stage02_SaveTempMovieQuotes();
            Console.WriteLine(DateTime.Now.ToString() + " - TempMovieQuotes saved.");

            BuildTempMovieFromTempTables();

            // Update stage
            Program.tempmoviestatus.Stage = 3;
            UpdateTempMovieStatus(Program.tempmoviestatus);

            Console.WriteLine(DateTime.Now.ToString() + " - Saving temp movie to temp tables complete.");
        }

        /// <summary>
        /// Saves the data in Program.tempmovie to the TempMovie table returning the ID of the record.        /// </summary>
        /// <returns>The ID of the newly created record in the TempMovie table.</returns>
        public static long Stage02_SaveTempMovie()
        {
            long id = Program.BLL_TempMovie.Insert(ref Program.tempmovie);
            return id;
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.BusinessData to the TempBusinessData table.
        /// </summary>
        /// <param name="id">The ID of the TempMovie from the TempMovie table.  Used to make sure record does not already exist.</param>
        public static void Stage02_SaveTempBusinessData(long id)
        {
            DA.Models.MovieDatabase.TempBusiness tempbusiness = Program.tempmovie.BusinessData;
            bool successful = false;
            do
            {
                try
                {
                    if (!Program.BLL_TempBusinessData.Contains(id))
                    {
                        Program.BLL_TempBusinessData.Insert(ref tempbusiness);
                        successful = Program.BLL_TempBusinessData.Contains(id);
                    }
                    else
                    {
                        successful = true;
                    }
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.TechnicalData to the TempTechnicalData table.
        /// </summary>
        /// <param name="id">The ID of the TempMovie from the TempMovie table.  Used to make sure record does not already exist.</param>
        public static void Stage02_SaveTempTechnialData(long id)
        {
            DA.Models.MovieDatabase.TempTechnical temptechnical = Program.tempmovie.TechnicalData;
            bool successful = false;
            do
            {
                try
                {
                    if (!Program.BLL_TempTechnicalData.Contains(id))
                    {
                        Program.BLL_TempTechnicalData.Insert(ref temptechnical);
                        successful = Program.BLL_TempTechnicalData.Contains(id);
                    }
                    else
                    {
                        successful = true;
                    }
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.Cast to the TempCastMembers table.
        /// </summary>
        public static void Stage02_SaveTempCastMembers()
        {
            DA.Models.MovieDatabase.TempCastMember tcm;
            foreach (DA.Models.MovieDatabase.TempCastMember c in Program.tempmovie.Cast)
            {
                bool successful = false;
                do
                {
                    try
                    {
                        tcm = c;
                        if (!Program.BLL_TempCastMembers.Contains(tcm.ActorIMDBID))
                        {
                            Program.BLL_TempCastMembers.Insert(ref tcm);
                            successful = Program.BLL_TempCastMembers.Contains(tcm.ActorIMDBID);
                        }
                        else
                        {
                            successful = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.Directors to the TempDirectors table.
        /// </summary>
        public static void Stage02_SaveTempDirectors()
        {
            DA.Models.MovieDatabase.TempDirector td;
            foreach (DA.Models.MovieDatabase.TempDirector d in Program.tempmovie.Directors)
            {
                bool successful = false;
                do
                {
                    try
                    {
                        td = d;
                        if (!Program.BLL_TempDirectors.Contains(td.DirectorIMDBID, td.Name))
                        {
                            Program.BLL_TempDirectors.Insert(ref td);
                            successful = Program.BLL_TempDirectors.Contains(td.DirectorIMDBID, td.Name);
                        }
                        else
                        {
                            successful = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.Writers to the TempWriters table.
        /// </summary>
        public static void Stage02_SaveTempWriters()
        {
            DA.Models.MovieDatabase.TempWriter tw;
            foreach (DA.Models.MovieDatabase.TempWriter w in Program.tempmovie.Writers)
            {
                bool successful = false;
                do
                {
                    try
                    {
                        tw = w;
                        if (!Program.BLL_TempWriters.Contains(tw.Name))
                        {
                            Program.BLL_TempWriters.Insert(ref tw);
                            successful = Program.BLL_TempWriters.Contains(tw.Name);
                        }
                        else
                        {
                            successful = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.SimpleTVEpisodes to the TempSimpleTVEpisodes table.
        /// </summary>
        public static void Stage02_SaveTempSimpleTVEpisodes()
        {
            DA.Models.MovieDatabase.TempSimpleTVEpisode stve;
            foreach (DA.Models.MovieDatabase.TempSimpleTVEpisode st in Program.tempmovie.SimpleTVEpisodes)
            {
                bool successful = false;
                do
                {
                    try
                    {
                        stve = st;
                        if (!Program.BLL_TempSimpleTVEpisodes.Contains(stve.EpisodeIMDBID))
                        {
                            Program.BLL_TempSimpleTVEpisodes.Insert(ref stve);
                            successful = Program.BLL_TempSimpleTVEpisodes.Contains(stve.EpisodeIMDBID);
                        }
                        else
                        {
                            successful = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.Countries to the TempCountries table.
        /// </summary>
        public static void Stage02_SaveTempCountries()
        {
            DA.Models.MovieDatabase.TempCountry tc;
            foreach (DA.Models.MovieDatabase.TempCountry c in Program.tempmovie.Countries)
            {
                bool successful = false;
                do
                {
                    try
                    {
                        tc = c;
                        if (!Program.BLL_TempCountries.Contains(tc.CountryName))
                        {
                            Program.BLL_TempCountries.Insert(ref tc);
                            successful = Program.BLL_TempCountries.Contains(tc.CountryName);
                        }
                        else
                        {
                            successful = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.FilmingLocations to the TempFilmingLocations table.
        /// </summary>
        public static void Stage02_SaveTempFilmingLocations()
        {
            DA.Models.MovieDatabase.TempFilmingLocation tfl;
            foreach (DA.Models.MovieDatabase.TempFilmingLocation f in Program.tempmovie.FilmingLocations)
            {
                bool successful = false;
                do
                {
                    try
                    {
                        tfl = f;
                        if (!Program.BLL_TempFilmingLocations.Contains(tfl.Location))
                        {
                            Program.BLL_TempFilmingLocations.Insert(ref tfl);
                            successful = Program.BLL_TempFilmingLocations.Contains(tfl.Location);
                        }
                        else
                        {
                            successful = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.Genres to the TempGenres table.
        /// </summary>
        public static void Stage02_SaveTempGenres()
        {
            DA.Models.MovieDatabase.TempGenre ge;
            foreach (DA.Models.MovieDatabase.TempGenre g in Program.tempmovie.Genres)
            {
                bool successful = false;
                do
                {
                    try
                    {
                        ge = g;
                        if (!Program.BLL_TempGenres.Contains(ge.GenreName))
                        {
                            Program.BLL_TempGenres.Insert(ref ge);
                            successful = Program.BLL_TempGenres.Contains(ge.GenreName);
                        }
                        else
                        {
                            successful = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.Languages to the TempLanguages table.
        /// </summary>
        public static void Stage02_SaveTempLanguages()
        {
            DA.Models.MovieDatabase.TempLanguage la;
            foreach (DA.Models.MovieDatabase.TempLanguage l in Program.tempmovie.Languages)
            {
                bool successful = false;
                do
                {
                    try
                    {
                        la = l;
                        if (!Program.BLL_TempLanguages.Contains(la.LanguageName))
                        {
                            Program.BLL_TempLanguages.Insert(ref la);
                            successful = Program.BLL_TempLanguages.Contains(la.LanguageName);
                        }
                        else
                        {
                            successful = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.Trivia to the TempMovieTrivia table.
        /// </summary>
        public static void Stage02_SaveTempMovieTrivia()
        {
            DA.Models.MovieDatabase.TempMovieTrivia tr;
            foreach (DA.Models.MovieDatabase.TempMovieTrivia t in Program.tempmovie.Trivia)
            {
                bool successful = false;
                do
                {
                    try
                    {
                        tr = t;
                        if (!Program.BLL_TempMovieTrivia.Contains(tr.Trivia))
                        {
                            Program.BLL_TempMovieTrivia.Insert(ref tr);
                            successful = Program.BLL_TempMovieTrivia.Contains(tr.Trivia);
                        }
                        else
                        {
                            successful = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.AKA to the TempAKA table.
        /// </summary>
        public static void Stage02_SaveTempAKA()
        {
            DA.Models.MovieDatabase.TempAKA ak;
            foreach (DA.Models.MovieDatabase.TempAKA a in Program.tempmovie.AKA)
            {
                bool successful = false;
                do
                {
                    try
                    {
                        ak = a;
                        if (!Program.BLL_TempAKAs.Contains(ak.Country, ak.Title))
                        {
                            Program.BLL_TempAKAs.Insert(ref ak);
                            successful = Program.BLL_TempAKAs.Contains(ak.Country, ak.Title);
                        }
                        else
                        {
                            successful = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.SimilarMovies to the TempSimilarMovies table.
        /// </summary>
        public static void Stage02_SaveTempSimilarMovie()
        {
            DA.Models.MovieDatabase.TempSimilarMovie sm;
            foreach (DA.Models.MovieDatabase.TempSimilarMovie s in Program.tempmovie.SimilarMovies)
            {
                bool successful = false;
                do
                {
                    try
                    {
                        sm = s;
                        if (!Program.BLL_TempSimilarMovies.Contains(sm.IMDBID))
                        {
                            Program.BLL_TempSimilarMovies.Insert(ref sm);
                            successful = Program.BLL_TempSimilarMovies.Contains(sm.IMDBID);
                        }
                        else
                        {
                            successful = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.Goofs to the TempGoofs table.
        /// </summary>
        public static void Stage02_SaveTempGoofs()
        {
            DA.Models.MovieDatabase.TempGoof gf;
            foreach (DA.Models.MovieDatabase.TempGoof g in Program.tempmovie.Goofs)
            {
                bool successful = false;
                do
                {
                    try
                    {
                        gf = g;
                        if (!Program.BLL_TempGoofs.Contains(gf.GoofText))
                        {
                            Program.BLL_TempGoofs.Insert(ref gf);
                            successful = Program.BLL_TempGoofs.Contains(gf.GoofText);
                        }
                        else
                        {
                            successful = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.Keywords to the TempKeywords table.
        /// </summary>
        public static void Stage02_SaveTempKeywords()
        {
            DA.Models.MovieDatabase.TempKeyword kw;
            foreach (String k in Program.tempmovie.Keywords)
            {
                bool successful = false;
                do
                {
                    try
                    {
                        kw = new DA.Models.MovieDatabase.TempKeyword();
                        kw.ID = -1;
                        kw.MovieID = -1;
                        kw.KeywordID = -1;
                        kw.Keyword = k;
                        if (!Program.BLL_TempKeywords.Contains(kw.Keyword))
                        {
                            Program.BLL_TempKeywords.Insert(ref kw);
                            successful = Program.BLL_TempKeywords.Contains(kw.Keyword);
                        }
                        else
                        {
                            successful = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Saves the data in Program.tempmovie.MovieQuotes to the TempMovieQuotes and TempLines tables.
        /// </summary>
        public static void Stage02_SaveTempMovieQuotes()
        {
            DA.Models.MovieDatabase.TempMovieQuote qu;
            DA.Models.MovieDatabase.TempLine ln;
            foreach (DA.Models.MovieDatabase.TempMovieQuote q in Program.tempmovie.MovieQuotes)
            {
                bool successful = false;
                qu = q;
                do
                {
                    try
                    {
                        Program.BLL_TempMovieQuotes.Insert(ref qu);
                        successful = true;
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);

                foreach (DA.Models.MovieDatabase.TempLine l in qu.Lines)
                {
                    successful = false;
                    do
                    {
                        try
                        {
                            ln = l;
                            if (!Program.BLL_TempLines.Contains(qu.TempQuoteID, ln.CharacterName, ln.LineText))
                            {
                                Program.BLL_TempLines.Insert(ref ln);
                                successful = Program.BLL_TempLines.Contains(qu.TempQuoteID, ln.CharacterName, ln.LineText);
                            }
                            else
                            {
                                successful = true;
                            }
                        }
                        catch (Exception ex)
                        {
                            successful = false;
                        }
                    } while (!successful);
                }
            }
        }

        #endregion

        #region Shared Method
        /// <summary>
        /// Rebuild the tempmovie structure from the database tables.
        /// </summary>
        public static void BuildTempMovieFromTempTables()
        {
            Console.WriteLine(DateTime.Now.ToString() + " - Building TempMovie from tables started...");
            Program.tempmovie = Program.BLL_TempMovie.SelectByID_model(1);

            // TempBusinessData
            DA.Models.MovieDatabase.TempBusiness tempbusiness = Program.BLL_TempBusinessData.SelectByID_model(1);
            Program.tempmovie.BusinessData = tempbusiness;

            // TempTechnicalData
            DA.Models.MovieDatabase.TempTechnical temptechnical = Program.BLL_TempTechnicalData.SelectByID_model(1);
            Program.tempmovie.TechnicalData = temptechnical;

            // TempCastMembers
            List<DA.Models.MovieDatabase.TempCastMember> cast = Program.BLL_TempCastMembers.SelectAll_list();
            Program.tempmovie.Cast = cast;

            // TempDirectors
            List<DA.Models.MovieDatabase.TempDirector> dir = Program.BLL_TempDirectors.SelectAll_list();
            Program.tempmovie.Directors = dir;

            // TempWriters
            List<DA.Models.MovieDatabase.TempWriter> wr = Program.BLL_TempWriters.SelectAll_list();
            Program.tempmovie.Writers = wr;

            // TempSimpleTVEpisodes
            List<DA.Models.MovieDatabase.TempSimpleTVEpisode> stv = Program.BLL_TempSimpleTVEpisodes.SelectAll_list();
            Program.tempmovie.SimpleTVEpisodes = stv;

            // TempCountries
            List<DA.Models.MovieDatabase.TempCountry> ct = Program.BLL_TempCountries.SelectAll_list();
            Program.tempmovie.Countries = ct;

            // TempFilmingLocations
            List<DA.Models.MovieDatabase.TempFilmingLocation> fl = Program.BLL_TempFilmingLocations.SelectAll_list();
            Program.tempmovie.FilmingLocations = fl;

            // TempGenres
            List<DA.Models.MovieDatabase.TempGenre> g = Program.BLL_TempGenres.SelectAll_list();
            Program.tempmovie.Genres = g;

            // TempLanguages
            List<DA.Models.MovieDatabase.TempLanguage> la = Program.BLL_TempLanguages.SelectAll_list();
            Program.tempmovie.Languages = la;

            // TempMovieTrivia
            List<DA.Models.MovieDatabase.TempMovieTrivia> tr = Program.BLL_TempMovieTrivia.SelectAll_list();
            Program.tempmovie.Trivia = tr;

            // TempAKA
            List<DA.Models.MovieDatabase.TempAKA> ak = Program.BLL_TempAKAs.SelectAll_list();
            Program.tempmovie.AKA = ak;

            // TempSimilarMovie
            List<DA.Models.MovieDatabase.TempSimilarMovie> sm = Program.BLL_TempSimilarMovies.SelectAll_list();
            Program.tempmovie.SimilarMovies = sm;

            // TempGoofs
            List<DA.Models.MovieDatabase.TempGoof> gf = Program.BLL_TempGoofs.SelectAll_list();
            Program.tempmovie.Goofs = gf;

            // TempKeywords
            List<DA.Models.MovieDatabase.TempKeyword> kw = Program.BLL_TempKeywords.SelectAll_list();
            Program.tempmovie.Keywords = new List<string>();
            foreach (DA.Models.MovieDatabase.TempKeyword k in kw)
            {
                Program.tempmovie.Keywords.Add(k.Keyword);
            }

            // TempMovieQuotes
            List<DA.Models.MovieDatabase.TempMovieQuote> quotes = Program.BLL_TempMovieQuotes.SelectAll_list();
            DA.Models.MovieDatabase.TempMovieQuote qu;
            DA.Models.MovieDatabase.TempLine ln;
            Program.tempmovie.MovieQuotes = new List<DA.Models.MovieDatabase.TempMovieQuote>();
            foreach (DA.Models.MovieDatabase.TempMovieQuote q in quotes)
            {
                qu = q;
                qu.Lines = new List<DA.Models.MovieDatabase.TempLine>();
                List<DA.Models.MovieDatabase.TempLine> lines = Program.BLL_TempLines.SelectByTempQuoteID_list(qu.TempQuoteID);
                foreach (DA.Models.MovieDatabase.TempLine l in lines)
                {
                    qu.Lines.Add(l);
                }
                Program.tempmovie.MovieQuotes.Add(qu);
            }
            Console.WriteLine(DateTime.Now.ToString() + " - Building TempMovie from tables complete.");
        }

        #endregion

        #region Stage03

        public void Stage03_AddCastMembersToPeopleTable()
        {
            if (Program.tempmovie == null)
            {
                BuildTempMovieFromTempTables();
            }

            Console.Title = "Stage 3 - " + Program.tempmovie.Title;

            Console.WriteLine(DateTime.Now.ToString() + " - Adding Cast Members to People table started...");

            for (int x = 0; x < Program.tempmovie.Cast.Count; ++x)
            {
                Console.WriteLine(DateTime.Now.ToString() + " - Cast Member " + (x + 1).ToString() + " of " + Program.tempmovie.Cast.Count.ToString() + " - " + Program.tempmovie.Cast[x].ActorName + " started...");

                DA.Models.MovieDatabase.TempCastMember c;
                if (!Program.tempmovie.Cast[x].InDatabase)
                {
                    c = Program.tempmovie.Cast[x];
                    long PersonID = -1;
                    do
                    {
                        PersonID = MovieDatabase.MovieDatabase.AddPerson(c.ActorIMDBID, false);
                    } while (PersonID < 0);

                    Program.person = null;
                    do
                    {
                        try
                        {
                            Program.person = Program.BLL_People.SelectByIMDBID_model(c.ActorIMDBID);
                        }
                        catch (Exception ex)
                        {
                            Program.person = null;
                        }
                    } while (Program.person == null);

                    c.PersonID = Program.person.PersonID;
                    c.InDatabase = true;

                    DA.Models.MovieDatabase.TempCastMember tc;
                    String tempimdbid = c.ActorIMDBID;
                    bool addsuccess = false;
                    do
                    {
                        try
                        {
                            Program.BLL_TempCastMembers.UpdateByActorIMDBID(ref c);
                            tc = Program.BLL_TempCastMembers.SelectByActorIMDBID_model(tempimdbid);
                            addsuccess = tc.PersonID == PersonID && tc.InDatabase;
                        }
                        catch (Exception ex)
                        {
                            addsuccess = false;
                        }
                    } while (!addsuccess);
                }
                Console.WriteLine(DateTime.Now.ToString() + " - " + Program.tempmovie.Cast[x].ActorName + " - Add to People complete.");
            }

            bool alladded = true;
            Console.WriteLine(DateTime.Now.ToString() + " - Checking that all Cast Members added...");
            for (int a = 0; a < Program.tempmovie.Cast.Count; ++a)
            {
                if (!Program.tempmovie.Cast[a].InDatabase)
                {
                    alladded = false;
                    Console.WriteLine(DateTime.Now.ToString() + " - All cast members NOT added.  Repeating.");
                }
            }

            if (alladded)
            {
                Console.WriteLine(DateTime.Now.ToString() + " - Checking that all Cast Members added complete.");
                Program.tempmoviestatus.Stage = 4;
                UpdateTempMovieStatus(Program.tempmoviestatus);
            }
        }

        #endregion

        #region Stage04

        public void Stage04_AddDirectorsToPeopleTable()
        {
            if (Program.tempmovie == null)
            {
                BuildTempMovieFromTempTables();
            }

            Console.Title = "Stage 4 - " + Program.tempmovie.Title;

            Console.WriteLine(DateTime.Now.ToString() + " - Adding Directors to People table beginning...");

            if (Program.tempmovie.Directors.Count > 0)
            {
                for (int x = 0; x < Program.tempmovie.Directors.Count; ++x)
                {
                    Console.WriteLine(DateTime.Now.ToString() + " - Director " + (x + 1).ToString() + " of " + Program.tempmovie.Directors.Count.ToString() + " - " + Program.tempmovie.Directors[x].Name + " started...");

                    DA.Models.MovieDatabase.TempDirector d;
                    if (!Program.tempmovie.Directors[x].InDatabase)
                    {
                        d = Program.tempmovie.Directors[x];

                        long PersonID = -1;
                        do
                        {
                            PersonID = MovieDatabase.MovieDatabase.AddPerson(d.DirectorIMDBID, false);
                        } while (PersonID < 0);

                        Program.person = null;
                        do
                        {
                            try
                            {
                                Program.person = Program.BLL_People.SelectByIMDBID_model(d.DirectorIMDBID);
                            }
                            catch (Exception ex)
                            {
                                Program.person = null;
                            }
                        } while (Program.person == null);

                        d.DirectorPersonID = Program.person.PersonID;
                        d.InDatabase = true;

                        if (d.ID != -1)
                        {
                            DA.Models.MovieDatabase.TempDirector td;
                            String tempimdbid = d.DirectorIMDBID;
                            bool addsuccess = false;
                            do
                            {
                                try
                                {
                                    Program.BLL_TempDirectors.Update(ref d);
                                    td = Program.BLL_TempDirectors.SelectByDirectorIMDBID_model(tempimdbid);
                                    addsuccess = td.DirectorPersonID == PersonID && td.InDatabase;
                                }
                                catch (Exception ex)
                                {
                                    addsuccess = false;
                                }
                            } while (!addsuccess);
                            Console.WriteLine(DateTime.Now.ToString() + " - " + Program.tempmovie.Directors[x].Name + " - Add to People complete.");
                        }
                        else
                        {
                            // Try rebuilding.
                            BuildTempMovieFromTempTables();
                        }
                    }

                    bool alladded = true;
                    Console.WriteLine(DateTime.Now.ToString() + " - Checking that all Directors added...");
                    for (int b = 0; b < Program.tempmovie.Directors.Count; ++b)
                    {
                        if (!Program.tempmovie.Directors[b].InDatabase)
                        {
                            alladded = false;
                            Console.WriteLine(DateTime.Now.ToString() + " - All directors NOT added.  Repeating.");
                        }
                    }

                    if (alladded)
                    {
                        Console.WriteLine(DateTime.Now.ToString() + " - Checking that all Directors added complete.");
                        Program.tempmoviestatus.Stage = 5;
                        UpdateTempMovieStatus(Program.tempmoviestatus);
                    }

                }
            }
            else
            {
                Console.WriteLine(DateTime.Now.ToString() + " - No directors.  Moving on.");
                Program.tempmoviestatus.Stage = 5;
                UpdateTempMovieStatus(Program.tempmoviestatus);
            }
        }

        #endregion

        #region Stage05

        public void Stage05_AddWritersToPeopleTable()
        {
            if (Program.tempmovie == null)
            {
                BuildTempMovieFromTempTables();
            }

            Console.Title = "Stage 5 - " + Program.tempmovie.Title;

            Console.WriteLine(DateTime.Now.ToString() + " - Adding writers to People table beginning...");

            if (Program.tempmovie.Writers.Count > 0)
            {
                for (int x = 0; x < Program.tempmovie.Writers.Count; ++x)
                {
                    Console.WriteLine(DateTime.Now.ToString() + " - Writers " + (x + 1).ToString() + " of " + Program.tempmovie.Writers.Count.ToString() + " - " + Program.tempmovie.Writers[x].Name + " started...");

                    DA.Models.MovieDatabase.TempWriter w;

                    if (!Program.tempmovie.Writers[x].InDatabase)
                    {
                        w = Program.tempmovie.Writers[x];

                        long PersonID = -1;
                        do
                        {
                            PersonID = MovieDatabase.MovieDatabase.AddPerson(w.WriterIMDBID, false);
                        } while (PersonID < 0);

                        Program.person = null;
                        do
                        {
                            try
                            {
                                Program.person = Program.BLL_People.SelectByIMDBID_model(w.WriterIMDBID);
                            }
                            catch (Exception ex)
                            {
                                Program.person = null;
                            }
                        } while (Program.person == null);

                        w.WriterPersonID = Program.person.PersonID;
                        w.InDatabase = true;

                        if (w.ID != -1)
                        {
                            DA.Models.MovieDatabase.TempWriter tw;
                            String tempimdbid = w.WriterIMDBID;
                            bool addsuccess = false;
                            do
                            {
                                try
                                {
                                    Program.BLL_TempWriters.Update(ref w);
                                    tw = Program.BLL_TempWriters.SelectByWriterIMDBID_model(tempimdbid);
                                    addsuccess = tw.WriterPersonID == PersonID && tw.InDatabase;
                                }
                                catch (Exception ex)
                                {
                                    addsuccess = false;
                                }
                            } while (addsuccess == false);
                            Console.WriteLine(DateTime.Now.ToString() + " - " + Program.tempmovie.Writers[x].Name + " - Add to People complete.");
                        }
                        else
                        {
                            // Try rebuilding.
                            BuildTempMovieFromTempTables();
                        }
                    }
                }

                bool alladded = true;
                Console.WriteLine(DateTime.Now.ToString() + " - Checking that all Writers added...");
                for (int b = 0; b < Program.tempmovie.Writers.Count; ++b)
                {
                    if (!Program.tempmovie.Writers[b].InDatabase)
                    {
                        alladded = false;
                        Console.WriteLine(DateTime.Now.ToString() + " - All writers NOT added.  Repeating.");
                    }
                }

                if (alladded)
                {
                    Console.WriteLine(DateTime.Now.ToString() + " - Checking that all Writers added complete.");
                    Program.tempmoviestatus.Stage = 6;
                    UpdateTempMovieStatus(Program.tempmoviestatus);
                }
            }
            else
            {
                Console.WriteLine(DateTime.Now.ToString() + " - No writers.  Moving on.");
                Program.tempmoviestatus.Stage = 6;
                UpdateTempMovieStatus(Program.tempmoviestatus);
            }
        }

        #endregion

        #region Stage06

        public void Stage06_AddMovieToMoviesTableAndUpdateMovieIDs()
        {
            if (Program.tempmovie == null)
            {
                BuildTempMovieFromTempTables();
            }

            Console.Title = "Stage 6 - " + Program.tempmovie.Title;

            bool InDatabase = false;
            long MovieID = -1;

            // Determine if the movie has already been added by IMDBID
            InDatabase = Program.BLL_Movies.Contains(Program.tempmovie.IMDBID);

            if (InDatabase)
            {
                // The movie already exists.  Lookup movie to get MovieID.
                Program.movie = Program.BLL_Movies.SelectByIMDBID_model(Program.tempmovie.IMDBID);
                MovieID = Program.movie.MovieID;
                Console.WriteLine(DateTime.Now.ToString() + " - The movie already exists in the Movies table.  Looking up MovieID.");
            }
            else
            {
                Console.WriteLine(DateTime.Now.ToString() + " - The movie does not exist in the Movies table.  Proceeding with add.");
                // The movie does not exist.  Proceed with add.
                MovieID = Stage06_AddMovieToMovieTable();
                Console.WriteLine(DateTime.Now.ToString() + " - Movie added to Movies table.");
            }

            Stage06_AdjustMoviePosterFilenames();
            Stage06_UpdateMovie();
            Console.WriteLine(DateTime.Now.ToString() + " - Movie filenames have been adjusted.");

            Stage06_DownloadMoviePosterLocally();
            Console.WriteLine(DateTime.Now.ToString() + " - Movie poster downloaded locally.");

            Stage06_UploadMoviePosterToWeb();
            Console.WriteLine(DateTime.Now.ToString() + " - Movie poster uploaded to web.");

            Stage06_SaveMoviePosterToDatabase();
            Console.WriteLine(DateTime.Now.ToString() + " - Movie poster added to MoviePosters table in the database.");

            // Now that the movie has been added to the Movies table,
            // update all temp tables with the new MovieID. This is in case
            // the add process is interrupted.  It can then continue upon
            // the next run.

            // Update TempMovie
            Stage06_UpdateTempMovie(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - TempMovie data updated.");

            // Update TempBusinessData
            Stage06_UpdateTempBusinessData(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Business data updated.");

            // Update TempTechnicalData
            Stage06_UpdateTempTechnicalData(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Technical data updated.");

            // Update TempCastMembers
            Stage06_UpdateTempCastMembers(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Cast updated.");

            // Update TempDirectors
            Stage06_UpdateTempDirectors(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Directors updated.");

            // Update TempWriters
            Stage06_UpdateTempWriters(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Writers updated.");

            // Update TempCountries
            Stage06_UpdateTempCountries(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Countries updated.");

            // Update TempFilmingLocations
            Stage06_UpdateTempFilmingLocations(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Filming locations updated.");

            // Update TempGenres
            Stage06_UpdateTempGenres(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Genres updated.");

            // Update TempLanguages
            Stage06_UpdateTempLanguages(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Languages updated.");

            // Update TempMovieTrivia
            Stage06_UpdateTempMovieTrivia(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Movie trivia updated.");

            // Update TempAKAs
            Stage06_UpdateTempAKAs(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - AKAs updated.");

            // Update TempSimilarMovies
            Stage06_UpdateTempSimilarMovies(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Similar movies updated.");

            // Update TempMovieQuotes
            Stage06_UpdateTempMovieQuotes(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Movie quotes updated.");

            // Update TempGoofs
            Stage06_UpdateTempGoofs(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Goofs movies updated.");

            // Update TempSimpleEpisodes
            Stage06_UpdateTempSimpleEpisodes(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Simple TV episodes updated.");

            // If this is a TV Episode, make sure the series
            // SimpleTVEpisodes EpisodeMovieID is updated.
            if (Program.tempmovie.Type.Equals("TV Episode"))
            {
                Stage06_UpdateEpisodeMovieID(MovieID);
                Console.WriteLine(DateTime.Now.ToString() + " - Series Simple TV episodes EpisodeMovieID updated.");
            }

            // Update Filmographies
             Stage06_UpdateFilmographies(Program.tempmovie.IMDBID, MovieID);

            Console.WriteLine(DateTime.Now.ToString() + " - Stage 6 complete.");

            Program.tempmoviestatus.Stage = 7;
            UpdateTempMovieStatus(Program.tempmoviestatus);
        }

        /// <summary>
        /// Add the base movie data stored in Program.tempmovie to the Movies table, returning the 
        /// MovieID of the newly added movie.
        /// </summary>
        /// <returns>The MovieID of the newly added movie.</returns>
        public static long Stage06_AddMovieToMovieTable()
        {
            DA.Models.MovieDatabase.Movie newmovie = new DA.Models.MovieDatabase.Movie();
            newmovie.IMDBID = Program.tempmovie.IMDBID;
            newmovie.Title = Program.tempmovie.Title;
            newmovie.OriginalTitle = Program.tempmovie.OriginalTitle;
            newmovie.Type = Program.tempmovie.Type;
            newmovie.MPAARating = Program.tempmovie.MPAARating;
            newmovie.ReleaseDate = Program.tempmovie.ReleaseDate;
            newmovie.Year = Program.tempmovie.Year;
            newmovie.Runtime = Program.tempmovie.Runtime;
            newmovie.Plot = Program.tempmovie.Plot;
            newmovie.SimplePlot = Program.tempmovie.SimplePlot;
            newmovie.Metascore = Program.tempmovie.Metascore;
            newmovie.Rating = Program.tempmovie.Rating;
            newmovie.IMDBURL = Program.tempmovie.IMDBURL;
            newmovie.IMDBPosterURL = Program.tempmovie.IMDBPosterURL;
            newmovie.Votes = Program.tempmovie.Votes;
            newmovie.SeriesIMDBID = Program.tempmovie.SeriesIMDBID;
            newmovie.SeriesName = Program.tempmovie.SeriesName;
            newmovie.PosterFilename = Program.tempmovie.PosterFilename;
            newmovie.LocalPosterFolder = Program.tempmovie.LocalPosterFolder;
            newmovie.LocalPosterPath = Program.tempmovie.LocalPosterFolder + "\\" + Program.tempmovie.PosterFilename;
            newmovie.WebPosterURL = Program.tempmovie.WebPosterURL;
            newmovie.MovieJSON = Program.tempmovie.MovieJSON;
            newmovie.DateLastUpdated = GetTimestamp();

            long movieid = -1;
            do
            {
                try
                {
                    movieid = Program.BLL_Movies.Insert(ref newmovie);
                    Program.movie = Program.BLL_Movies.SelectByIMDBID_model(Program.tempmovie.IMDBID);
                    if (Program.movie != null)
                    {
                        movieid = Program.movie.MovieID;
                    }
                    else
                    {
                        movieid = -1;
                    }
                }
                catch (Exception ex)
                {
                    movieid = -1;
                }
            } while (movieid == -1);

            return movieid;
        }

        /// <summary>
        /// Adjust movie poster filenames and paths for final save.
        /// </summary>
        public static void Stage06_AdjustMoviePosterFilenames()
        {
            // Get original IMDB Photo URL and use it to adjust
            // filenames and extensions.
            // Filename will look like:
            // https://m.media-amazon.com/images/M/MV5BMTkxODc5ODI5OV5BMl5BanBnXkFtZTcwNzExOTUyNw@@._V1.jpg
            DA.Models.MovieDatabase.WorkerConfiguration config = Program.BLL_WorkerConfiguration.SelectByID_model(1);
            Program.movie.LocalPosterFolder = config.MovieLocalPosterFolder;
            if (Program.movie.Type.Equals("TV Episode"))
            {
                if (!Program.movie.IMDBPosterURL.Equals(""))
                {
                    Program.movie.PosterFilename = Program.movie.SeriesName + "_" + Program.movie.Title.Replace(" ", "_") + "_(" + Program.movie.Year + ")_" + Program.movie.MovieID.ToString() + Program.movie.IMDBPosterURL.Substring(Program.movie.IMDBPosterURL.LastIndexOf("."), Program.movie.IMDBPosterURL.Length - Program.movie.IMDBPosterURL.LastIndexOf("."));
                }
                else
                {
                    Program.movie.PosterFilename = Program.movie.SeriesName + "_" + Program.movie.Title.Replace(" ", "_") + "_(" + Program.movie.Year + ")_" + Program.movie.MovieID.ToString() + ".png";
                }

                Program.movie.PosterFilename = StripInvalidCharactersFromFilename(Program.movie.PosterFilename);
                Program.movie.LocalPosterPath = Program.movie.LocalPosterFolder + "\\" + Program.movie.PosterFilename;
                Program.movie.WebPosterURL = config.MovieWebPosterURLBase + Program.movie.PosterFilename;
            }
            else
            {
                if (!Program.movie.IMDBPosterURL.Equals(""))
                {
                    Program.movie.PosterFilename = Program.movie.Title.Replace(" ", "_") + "_(" + Program.movie.Year + ")_" + Program.movie.MovieID.ToString() + Program.movie.IMDBPosterURL.Substring(Program.movie.IMDBPosterURL.LastIndexOf("."), Program.movie.IMDBPosterURL.Length - Program.movie.IMDBPosterURL.LastIndexOf("."));
                }
                else
                {
                    Program.movie.PosterFilename = Program.movie.Title.Replace(" ", "_") + "_(" + Program.movie.Year + ")_" + Program.movie.MovieID.ToString() + ".png";
                }

                Program.movie.PosterFilename = StripInvalidCharactersFromFilename(Program.movie.PosterFilename);
                Program.movie.LocalPosterPath = Program.movie.LocalPosterFolder + "\\" + Program.movie.PosterFilename;
                Program.movie.WebPosterURL = config.MovieWebPosterURLBase + Program.movie.PosterFilename;
            }
        }

        /// <summary>
        /// Update the Movies table with the base data in Program.movie. 
        /// </summary>
        public static void Stage06_UpdateMovie()
        {
            bool successful = false;
            do
            {
                try
                {
                    Program.BLL_Movies.Update(ref Program.movie);
                    DA.Models.MovieDatabase.Movie tm = Program.BLL_Movies.SelectByMovieID_model(Program.movie.MovieID);
                    successful = Program.movie.LocalPosterPath.Equals(tm.LocalPosterPath);
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Saves the movie poster to a file in the location stored in the WorkerConfiguration table.
        /// </summary>
        public static void Stage06_DownloadMoviePosterLocally()
        {
            if (Program.movie.IMDBPosterURL != null && !Program.movie.IMDBPosterURL.Equals(""))
            {
                do
                {
                    try
                    {
                        if (!File.Exists(Program.movie.LocalPosterPath))
                        {
                            System.Drawing.Bitmap b;
                            Uri uri = new Uri(Program.movie.IMDBPosterURL);
                            WebRequest webRequest = (HttpWebRequest)WebRequest.Create(uri);
                            using (HttpWebResponse webResponse = (HttpWebResponse)webRequest.GetResponse())
                            {
                                using (Stream stream = webResponse.GetResponseStream())
                                {
                                    System.Drawing.Image img = System.Drawing.Image.FromStream(stream);
                                    img.Save(Program.movie.LocalPosterPath);
                                    img.Dispose();
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {

                    }
                } while (!File.Exists(Program.movie.LocalPosterPath));
            }
            else
            {
                // IMDB does not have a photo for this movie.  Point it to blank.
                DA.Models.MovieDatabase.WorkerConfiguration config = Program.BLL_WorkerConfiguration.SelectByID_model(1);
                File.Copy(config.MovieBlankPosterPath, Program.movie.LocalPosterPath, true);
            }
        }

        /// <summary>
        /// Uploads the recently saved movie poster to the web server.
        /// </summary>
        public static void Stage06_UploadMoviePosterToWeb()
        {
            bool uploadsuccess = false;
            do
            {
                try
                {
                    uploadsuccess = FTPUploadFile(Program.movie.LocalPosterPath, "ftp://kelp.arvixe.com/mjafileserver.com/wwwroot/MovieDatabase/Photos/MoviePosters/" + Program.movie.PosterFilename, "program", "gm44ad");
                }
                catch (Exception ex)
                {
                    uploadsuccess = false;
                }
            } while (!uploadsuccess);
        }

        /// <summary>
        /// Saves the movie poster binary file to the database.
        /// </summary>
        public static void Stage06_SaveMoviePosterToDatabase()
        {
            DA.Models.MovieDatabase.MoviePoster photo = new DA.Models.MovieDatabase.MoviePoster();
            photo.MovieID = Program.movie.MovieID;
            photo.IMDBPosterURL = Program.movie.IMDBPosterURL;

            if (!photo.IMDBPosterURL.Equals(""))
            {
                photo.OriginalIMDBFilename = photo.IMDBPosterURL.Substring(photo.IMDBPosterURL.LastIndexOf("/"));
            }
            else
            {
                photo.OriginalIMDBFilename = "";
            }

            photo.PosterFilename = Program.movie.PosterFilename;
            photo.FilenameExtension = Program.movie.PosterFilename.Substring(Program.movie.PosterFilename.LastIndexOf("."));

            try
            {
                Image tempphoto = Image.FromFile(Program.movie.LocalPosterFolder + "\\" + Program.movie.PosterFilename);
                photo.Width = tempphoto.Width;
                photo.Height = tempphoto.Height;
                tempphoto.Dispose();
            }
            catch (Exception ex)
            {
                photo.Width = -1;
                photo.Height = -1;
            }

            FileInfo fi = new FileInfo(Program.movie.LocalPosterFolder + "\\" + Program.movie.PosterFilename);
            photo.MIMEType = "";
            photo.Size = fi.Length;

            byte[] file = ConvertFileToByteArray(Program.movie.LocalPosterFolder + "\\" + Program.movie.PosterFilename);
            photo.PhotoData = file;

            bool successful = false;
            do
            {
                if (Program.BLL_MoviePosters.Contains(Program.movie.MovieID))
                {
                    successful = true;
                }
                else
                {
                    Program.BLL_MoviePosters.Insert(ref photo);
                    successful = Program.BLL_MoviePosters.Contains(Program.movie.MovieID);
                }
            } while (!successful);
        }

        /// <summary>
        /// Uploads the TempMovie table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempMovie(long movieid)
        {
            Program.tempmovie.MovieID = movieid;
            Program.BLL_TempMovie.Update(ref Program.tempmovie);
        }

        /// <summary>
        /// Uploads the TempBusinessData table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempBusinessData(long movieid)
        {
            Program.tempmovie.BusinessData.MovieID = movieid;
            DA.Models.MovieDatabase.TempBusiness tb = Program.tempmovie.BusinessData;
            Program.BLL_TempBusinessData.Update(ref tb);
        }

        /// <summary>
        /// Uploads the TempTechnicalData table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempTechnicalData(long movieid)
        {
            Program.tempmovie.TechnicalData.MovieID = movieid;
            DA.Models.MovieDatabase.TempTechnical tt = Program.tempmovie.TechnicalData;
            Program.BLL_TempTechnicalData.Update(ref tt);
        }

        /// <summary>
        /// Uploads the TempCastMembers table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempCastMembers(long movieid)
        {
            DA.Models.MovieDatabase.TempCastMember tcm;
            foreach (DA.Models.MovieDatabase.TempCastMember c in Program.tempmovie.Cast)
            {
                DA.Models.MovieDatabase.TempCastMember ttcm = c;
                ttcm.MovieID = movieid;
                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TempCastMembers.Update(ref ttcm);
                        tcm = Program.BLL_TempCastMembers.SelectByID_model(c.ID);
                        successful = (tcm.MovieID == ttcm.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Uploads the TempDirectors table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempDirectors(long movieid)
        {
            DA.Models.MovieDatabase.TempDirector td;
            foreach (DA.Models.MovieDatabase.TempDirector d in Program.tempmovie.Directors)
            {
                DA.Models.MovieDatabase.TempDirector tdd = d;
                tdd.MovieID = movieid;
                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TempDirectors.Update(ref tdd);
                        td = Program.BLL_TempDirectors.SelectByID_model(tdd.ID);
                        successful = (td.MovieID == tdd.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Uploads the TempWriters table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempWriters(long movieid)
        {
            DA.Models.MovieDatabase.TempWriter tw;
            foreach (DA.Models.MovieDatabase.TempWriter w in Program.tempmovie.Writers)
            {
                DA.Models.MovieDatabase.TempWriter tww = w;
                tww.MovieID = movieid;
                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TempWriters.Update(ref tww);
                        tw = Program.BLL_TempWriters.SelectByID_model(tww.ID);
                        successful = (tw.MovieID == tww.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Uploads the TempCountries table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempCountries(long movieid)
        {
            DA.Models.MovieDatabase.TempCountry tc;
            foreach (DA.Models.MovieDatabase.TempCountry c in Program.tempmovie.Countries)
            {
                DA.Models.MovieDatabase.TempCountry tcc = c;
                tcc.MovieID = movieid;
                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TempCountries.Update(ref tcc);
                        tc = Program.BLL_TempCountries.SelectByID_model(tcc.ID);
                        successful = (tc.MovieID == tcc.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Uploads the TempFilmingLocations table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempFilmingLocations(long movieid)
        {
            DA.Models.MovieDatabase.TempFilmingLocation tfl;
            foreach (DA.Models.MovieDatabase.TempFilmingLocation f in Program.tempmovie.FilmingLocations)
            {
                DA.Models.MovieDatabase.TempFilmingLocation ttfl = f;
                ttfl.MovieID = movieid;
                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TempFilmingLocations.Update(ref ttfl);
                        tfl = Program.BLL_TempFilmingLocations.SelectByID_model(ttfl.ID);
                        successful = (tfl.MovieID == ttfl.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Uploads the TempGenres table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempGenres(long movieid)
        {
            DA.Models.MovieDatabase.TempGenre tg;
            foreach (DA.Models.MovieDatabase.TempGenre g in Program.tempmovie.Genres)
            {
                DA.Models.MovieDatabase.TempGenre ttg = g;
                ttg.MovieID = movieid;
                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TempGenres.Update(ref ttg);
                        tg = Program.BLL_TempGenres.SelectByID_model(ttg.ID);
                        successful = (tg.MovieID == ttg.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Uploads the TempLanguages table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempLanguages(long movieid)
        {
            DA.Models.MovieDatabase.TempLanguage tl;
            foreach (DA.Models.MovieDatabase.TempLanguage l in Program.tempmovie.Languages)
            {
                DA.Models.MovieDatabase.TempLanguage ttl = l;
                ttl.MovieID = movieid;
                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TempLanguages.Update(ref ttl);
                        tl = Program.BLL_TempLanguages.SelectByID_model(ttl.ID);
                        successful = (tl.MovieID == ttl.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Uploads the TempMovieTrivia table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempMovieTrivia(long movieid)
        {
            DA.Models.MovieDatabase.TempMovieTrivia tmt;
            foreach (DA.Models.MovieDatabase.TempMovieTrivia mt in Program.tempmovie.Trivia)
            {
                DA.Models.MovieDatabase.TempMovieTrivia ttmt = mt;
                ttmt.MovieID = movieid;
                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TempMovieTrivia.Update(ref ttmt);
                        tmt = Program.BLL_TempMovieTrivia.SelectByID_model(ttmt.ID);
                        successful = (tmt.MovieID == ttmt.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Uploads the TempAKAs table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempAKAs(long movieid)
        {
            DA.Models.MovieDatabase.TempAKA ta;
            foreach (DA.Models.MovieDatabase.TempAKA aka in Program.tempmovie.AKA)
            {
                DA.Models.MovieDatabase.TempAKA tta = aka;
                tta.MovieID = movieid;
                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TempAKAs.Update(ref tta);
                        ta = Program.BLL_TempAKAs.SelectByID_model(tta.ID);
                        successful = (ta.MovieID == tta.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Uploads the TempSimilarMovies table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempSimilarMovies(long movieid)
        {
            DA.Models.MovieDatabase.TempSimilarMovie tsm;
            foreach (DA.Models.MovieDatabase.TempSimilarMovie sm in Program.tempmovie.SimilarMovies)
            {
                DA.Models.MovieDatabase.TempSimilarMovie ttsm = sm;
                ttsm.MovieID = movieid;
                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TempSimilarMovies.Update(ref ttsm);
                        tsm = Program.BLL_TempSimilarMovies.SelectByID_model(ttsm.ID);
                        successful = (tsm.MovieID == ttsm.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Uploads the TempMovieQuotes table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempMovieQuotes(long movieid)
        {
            DA.Models.MovieDatabase.TempMovieQuote tmq;
            foreach (DA.Models.MovieDatabase.TempMovieQuote mq in Program.tempmovie.MovieQuotes)
            {
                DA.Models.MovieDatabase.TempMovieQuote ttmq = mq;
                ttmq.MovieID = movieid;
                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TempMovieQuotes.Update(ref ttmq);
                        tmq = Program.BLL_TempMovieQuotes.SelectByTempQuoteID_model(ttmq.TempQuoteID);
                        successful = (tmq.MovieID == ttmq.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Uploads the TempGoofs table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempGoofs(long movieid)
        {
            DA.Models.MovieDatabase.TempGoof tgo;
            foreach (DA.Models.MovieDatabase.TempGoof p in Program.tempmovie.Goofs)
            {
                DA.Models.MovieDatabase.TempGoof ttgo = p;
                ttgo.MovieID = movieid;
                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TempGoofs.Update(ref ttgo);
                        tgo = Program.BLL_TempGoofs.SelectByID_model(ttgo.ID);
                        successful = (tgo.MovieID == ttgo.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Uploads the TempSimpleEpisodes table with the given MovieID.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateTempSimpleEpisodes(long movieid)
        {
            DA.Models.MovieDatabase.TempSimpleTVEpisode tse;
            foreach (DA.Models.MovieDatabase.TempSimpleTVEpisode ep in Program.tempmovie.SimpleTVEpisodes)
            {
                DA.Models.MovieDatabase.TempSimpleTVEpisode ttse = ep;
                ttse.SeriesMovieID = movieid;
                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TempSimpleTVEpisodes.Update(ref ttse);
                        tse = Program.BLL_TempSimpleTVEpisodes.SelectByID_model(ttse.ID);
                        successful = (tse.SeriesMovieID == ttse.SeriesMovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// If this is a TV Episode, make sure the series SimpleTVEpisodes EpisodeMovieID is updated.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateEpisodeMovieID(long movieid)
        {
            DA.Models.MovieDatabase.SimpleTVEpisode[] tve;
            DA.Models.MovieDatabase.SimpleTVEpisode ttve;
            // Look up the series SimpleTVEpisodes records by SeriesIMDBID.
            tve = Program.BLL_SimpleTVEpisodes.SelectBySeriesIMDBID_model(Program.tempmovie.SeriesIMDBID);

            // Loop through the resulting SimpleTVEpisodes list and find the
            // record with the coresponding EpisodeIMDBID.  Update
            // the EpisodeMovieID.
            for (int b = 0; b < tve.Length; ++b)
            {
                if (tve[b].EpisodeIMDBID.Equals(Program.tempmovie.IMDBID))
                {
                    tve[b].EpisodeMovieID = movieid;

                    bool successful = false;
                    do
                    {
                        try
                        {
                            Program.BLL_SimpleTVEpisodes.Update(ref tve[b]);
                            ttve = Program.BLL_SimpleTVEpisodes.SelectBySimpleTVEpisodeID_model(tve[b].SimpleTVEpisodeID);
                            successful = (tve[b].EpisodeMovieID == movieid);
                        }
                        catch (Exception ex)
                        {
                            successful = false;
                        }
                    } while (!successful);
                }
            }
        }

        /// <summary>
        /// Uploads the Filmographies table by updating the MovieID of any record with the given IMDBID
        /// </summary>
        /// <param name="imdbid">The IMDBID of the records to be updated.</param>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage06_UpdateFilmographies(String imdbid, long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            do
            {
                try
                {
                    sSql = "UPDATE Filmographies SET MovieID = " + movieid.ToString() + " WHERE IMDBID = '" + imdbid + "'";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        #endregion

        #region Stage07

        public void Stage07_AddFinalTables()
        {
            Console.WriteLine(DateTime.Now.ToString() + " - Final save beginning...");

            if (Program.tempmovie == null)
            {
                do
                {
                    BuildTempMovieFromTempTables();
                } while (Program.tempmovie == null);
            }
            Console.WriteLine(DateTime.Now.ToString() + " - Temp movie loaded.");

            if (Program.movie == null)
            {
                do
                {
                    Program.movie = Program.BLL_Movies.SelectByIMDBID_model(Program.tempmovie.IMDBID);
                } while (Program.movie == null);
            }
            Console.WriteLine(DateTime.Now.ToString() + " - Movie loaded.");

            Console.Title = "Stage 7 - " + Program.tempmovie.Title;

            bool QuerySuccess = false;
            bool InDatabase = false;
            long MovieID = -1;

            MovieID = Program.movie.MovieID;

            // Save BusinessData
            Stage07_SaveBusinessData();
            Console.WriteLine(DateTime.Now.ToString() + " - Business data saved.");

            // Save TechnicalData
            Stage07_SaveTechnicalData();
            Console.WriteLine(DateTime.Now.ToString() + " - Technical data saved.");

            // Save Cast Members
            Stage07_SaveCastMembers(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Cast saved.");

            // Save Directors
            Stage07_SaveDirectors(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Directors saved.");

            // Save Writers
            Stage07_SaveWriters(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Writers saved.");

            // Save Countries
            Stage07_SaveCountries(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Countries saved.");

            // Save Filming Locations
            Stage07_SaveFilmingLocations(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Filming locations saved.");

            // Save Genres
            Stage07_SaveGenres(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Genres saved.");

            // Save Languages
            Stage07_SaveLanguages(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Languages saved.");

            // Save Movie Trivia
            Stage07_SaveMovieTrivia(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Movie trivia saved.");

            // Save AKAs
            Stage07_SaveAKAs(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - AKAs saved.");

            // Save Similar Movies
            Stage07_SaveSimilarMovies(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Similar movies saved.");

            // Save Movie Quotes
            Stage07_SaveMovieQuotes(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Movie quotes saved.");

            // Save Goofs
            Stage07_SaveGoofs(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Goofs saved.");

            // Save Keywords
            Stage07_SaveKeywords(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Keywords saved.");

            // Save Simple Episodes
            Stage07_SaveSimpleEpisodes(MovieID);
            Console.WriteLine(DateTime.Now.ToString() + " - Simple episodes saved.");

            Console.WriteLine(DateTime.Now.ToString() + " - Final save complete.");

            Program.tempmoviestatus.Stage = 8;
            UpdateTempMovieStatus(Program.tempmoviestatus);
        }

        /// <summary>
        /// Final save of BusinessData.
        /// </summary>
        public static void Stage07_SaveBusinessData()
        {
            DA.Models.MovieDatabase.Business b = null;
            if (Program.BLL_BusinessData.Contains(Program.tempmovie.MovieID))
            {
                do
                {
                    b = Program.BLL_BusinessData.SelectByMovieID_model(Program.tempmovie.MovieID);
                } while (b == null);

                b.MovieID = Program.tempmovie.MovieID;
                b.Budget = Program.tempmovie.BusinessData.Budget;
                b.OpeningWeekend = Program.tempmovie.BusinessData.OpeningWeekend;
                b.GrossUSA = Program.tempmovie.BusinessData.GrossUSA;
                b.Worldwide = Program.tempmovie.BusinessData.Worldwide;

                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_BusinessData.Update(ref b);
                        successful = true;
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
            else
            {
                b = new DA.Models.MovieDatabase.Business();
                b.MovieID = Program.tempmovie.MovieID;
                b.Budget = Program.tempmovie.BusinessData.Budget;
                b.OpeningWeekend = Program.tempmovie.BusinessData.OpeningWeekend;
                b.GrossUSA = Program.tempmovie.BusinessData.GrossUSA;
                b.Worldwide = Program.tempmovie.BusinessData.Worldwide;

                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_BusinessData.Insert(ref b);
                        successful = Program.BLL_BusinessData.Contains(b.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Final save of TechnicalData.
        /// </summary>
        public static void Stage07_SaveTechnicalData()
        {
            DA.Models.MovieDatabase.Technical t = null;
            if (Program.BLL_TechnicalData.Contains(Program.tempmovie.MovieID))
            {
                do
                {
                    t = Program.BLL_TechnicalData.SelectByMovieID_model(Program.tempmovie.MovieID);
                } while (t == null);

                t.MovieID = Program.tempmovie.MovieID;
                t.Runtime = Program.tempmovie.TechnicalData.Runtime;
                t.SoundMix = Program.tempmovie.TechnicalData.SoundMix;
                t.Color = Program.tempmovie.TechnicalData.Color;
                t.AspectRatio = Program.tempmovie.TechnicalData.AspectRatio;
                t.Camera = Program.tempmovie.TechnicalData.Camera;
                t.Laboratory = Program.tempmovie.TechnicalData.Laboratory;
                t.FilmLength = Program.tempmovie.TechnicalData.FilmLength;
                t.NegativeFormat = Program.tempmovie.TechnicalData.NegativeFormat;
                t.CinematographicProcess = Program.tempmovie.TechnicalData.CinematographicProcess;
                t.PrintedFilmFormat = Program.tempmovie.TechnicalData.PrintedFilmFormat;

                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TechnicalData.Update(ref t);
                        successful = true;
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
            else
            {
                t = new DA.Models.MovieDatabase.Technical();
                t.MovieID = Program.tempmovie.MovieID;
                t.Runtime = Program.tempmovie.TechnicalData.Runtime;
                t.SoundMix = Program.tempmovie.TechnicalData.SoundMix;
                t.Color = Program.tempmovie.TechnicalData.Color;
                t.AspectRatio = Program.tempmovie.TechnicalData.AspectRatio;
                t.Camera = Program.tempmovie.TechnicalData.Camera;
                t.Laboratory = Program.tempmovie.TechnicalData.Laboratory;
                t.FilmLength = Program.tempmovie.TechnicalData.FilmLength;
                t.NegativeFormat = Program.tempmovie.TechnicalData.NegativeFormat;
                t.CinematographicProcess = Program.tempmovie.TechnicalData.CinematographicProcess;
                t.PrintedFilmFormat = Program.tempmovie.TechnicalData.PrintedFilmFormat;

                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TechnicalData.Insert(ref t);
                        successful = Program.BLL_TechnicalData.Contains(t.MovieID);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// Final save of CastMembers using SQL joins and updates.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveCastMembers(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            do
            {
                try
                {
                    sSql = "DELETE FROM [dbo].[CastMembers] WHERE MovieID = " + movieid.ToString();
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "UPDATE TCM SET TCM.PersonID = P.PersonID FROM TempCastMembers TCM INNER JOIN People P ON TCM.ActorIMDBID = P.IMDBID";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "INSERT INTO[CastMembers] ([PersonID], [MovieID], [ActorIMDBID], [MovieIMDBID], [ActorName], [CharacterName], [Main], [IMDBCharacterURL], [IMDBPhotoURL], [IMDBActorProfileURL], [Ordinal]) ";
                    sSql = sSql + "SELECT [PersonID], [MovieID], [ActorIMDBID], [MovieIMDBID], [ActorName], [CharacterName], [Main], [IMDBCharacterURL], [IMDBPhotoURL], [IMDBActorProfileURL], [Ordinal] ";
                    sSql = sSql + "FROM[TempCastMembers] ";
                    sSql = sSql + "ORDER BY[TempCastMembers].[Ordinal] ";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Final save of Directors using SQL joins and updates.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveDirectors(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            do
            {
                try
                {
                    sSql = "DELETE FROM [dbo].[Directors] WHERE MovieID = " + movieid.ToString();
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "UPDATE TD SET TD.DirectorPersonID = P.PersonID FROM TempDirectors TD INNER JOIN People P ON TD.DirectorIMDBID = P.IMDBID";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "INSERT INTO [Directors] ([MovieID], [MovieIMDBID], [DirectorIMDBID], [DirectorPersonID], [Ordinal]) ";
                    sSql = sSql + "SELECT [MovieID], [MovieIMDBID], [DirectorIMDBID], [DirectorPersonID], [Ordinal] ";
                    sSql = sSql + "FROM[TempDirectors] ";
                    sSql = sSql + "ORDER BY[TempDirectors].[Ordinal] ";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Final save of Writers using SQL joins and updates.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveWriters(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            do
            {
                try
                {
                    sSql = "DELETE FROM [dbo].[Writers] WHERE MovieID = " + movieid.ToString();
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "UPDATE TW SET TW.WriterPersonID = P.PersonID FROM TempWriters TW INNER JOIN People P ON TW.WriterIMDBID = P.IMDBID";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "INSERT INTO [Writers] ([MovieID], [MovieIMDBID], [WriterIMDBID], [WriterPersonID], [Ordinal]) ";
                    sSql = sSql + "SELECT [MovieID], [MovieIMDBID], [WriterIMDBID], [WriterPersonID], [Ordinal] ";
                    sSql = sSql + "FROM[TempWriters] ";
                    sSql = sSql + "ORDER BY[TempWriters].[Ordinal] ";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Final save of Countries using SQL joins and updates.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveCountries(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            do
            {
                try
                {
                    sSql = "DELETE FROM [dbo].[Countries] WHERE MovieID = " + movieid.ToString();
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "INSERT INTO [Countries] ([MovieID], [CountryName], [Ordinal]) ";
                    sSql = sSql + "SELECT [MovieID], [CountryName], [Ordinal] ";
                    sSql = sSql + "FROM[TempCountries] ";
                    sSql = sSql + "ORDER BY[TempCountries].[Ordinal] ";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Final save of FilmingLocations using SQL joins and updates.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveFilmingLocations(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            do
            {
                try
                {
                    sSql = "DELETE FROM [dbo].[FilmingLocations] WHERE MovieID = " + movieid.ToString();
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "INSERT INTO [FilmingLocations] ([MovieID], [Location], [Remarks], [Ordinal]) ";
                    sSql = sSql + "SELECT [MovieID], [Location], [Remarks], [Ordinal] ";
                    sSql = sSql + "FROM[TempFilmingLocations] ";
                    sSql = sSql + "ORDER BY[TempFilmingLocations].[Ordinal] ";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Final save of Genres using SQL joins and updates.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveGenres(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            do
            {
                try
                {
                    sSql = "DELETE FROM [dbo].[Genres] WHERE MovieID = " + movieid.ToString();
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "INSERT INTO [Genres] ([MovieID], [GenreName], [Ordinal]) ";
                    sSql = sSql + "SELECT [MovieID], [GenreName], [Ordinal] ";
                    sSql = sSql + "FROM[TempGenres] ";
                    sSql = sSql + "ORDER BY[TempGenres].[Ordinal] ";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Final save of Languages using SQL joins and updates.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveLanguages(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            do
            {
                try
                {
                    sSql = "DELETE FROM [dbo].[Languages] WHERE MovieID = " + movieid.ToString();
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "INSERT INTO [Languages] ([MovieID], [LanguageName], [Ordinal]) ";
                    sSql = sSql + "SELECT [MovieID], [LanguageName], [Ordinal] ";
                    sSql = sSql + "FROM[TempLanguages] ";
                    sSql = sSql + "ORDER BY[TempLanguages].[Ordinal] ";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Final save of MovieTrivia using SQL joins and updates.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveMovieTrivia(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            do
            {
                try
                {
                    sSql = "DELETE FROM [dbo].[MovieTrivia] WHERE MovieID = " + movieid.ToString();
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "INSERT INTO [MovieTrivia] ([MovieID], [Trivia], [Ordinal]) ";
                    sSql = sSql + "SELECT [MovieID], [Trivia], [Ordinal] ";
                    sSql = sSql + "FROM[TempMovieTrivia] ";
                    sSql = sSql + "ORDER BY[TempMovieTrivia].[Ordinal] ";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Final save of AKAs using SQL joins and updates.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveAKAs(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            do
            {
                try
                {
                    sSql = "DELETE FROM [dbo].[AKAs] WHERE MovieID = " + movieid.ToString();
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "INSERT INTO [AKAs] ([MovieID], [Country], [Title], [Comment], [Ordinal]) ";
                    sSql = sSql + "SELECT [MovieID], [Country], [Title], [Comment], [Ordinal] ";
                    sSql = sSql + "FROM[TempAKAs] ";
                    sSql = sSql + "ORDER BY[TempAKAs].[Ordinal] ";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Final save of SimilarMovies using SQL joins and updates.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveSimilarMovies(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            do
            {
                try
                {
                    sSql = "DELETE FROM [dbo].[SimilarMovies] WHERE MovieID = " + movieid.ToString();
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "INSERT INTO [SimilarMovies] ([MovieID], [IMDBID], [MovieName], [Ordinal]) ";
                    sSql = sSql + "SELECT [MovieID], [IMDBID], [MovieName], [Ordinal] ";
                    sSql = sSql + "FROM[TempSimilarMovies] ";
                    sSql = sSql + "ORDER BY[TempSimilarMovies].[Ordinal] ";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Final save of MovieQuotes using SQL joins and updates.
        /// Only INSERT is handled for now.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveMovieQuotes(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            DA.Models.MovieDatabase.MovieQuote[] mqs = null;
            DA.Models.MovieDatabase.MovieQuote mq = null;
            DA.Models.MovieDatabase.TempLine[] templines = null;
            DA.Models.MovieDatabase.Line line = null;
            long quoteid = -1;
            long lineid = -1;
            for (int a = 0; a < Program.tempmovie.MovieQuotes.Count; ++a)
            {
                mq = new DA.Models.MovieDatabase.MovieQuote();
                mq.MovieID = Program.tempmovie.MovieID;
                mq.IMDBID = Program.tempmovie.IMDBID;
                mq.QuoteOrdinal = Program.tempmovie.MovieQuotes[a].QuoteOrdinal;

                successful = false;
                do
                {
                    try
                    {
                        quoteid = -1;
                        quoteid = Program.BLL_MovieQuotes.Insert(ref mq);
                        successful = (quoteid != -1);
                    }
                    catch (Exception ex)
                    {
                        successful = false;
                    }
                } while (!successful);

                templines = Program.BLL_TempLines.SelectByTempQuoteID_model(Program.tempmovie.MovieQuotes[a].TempQuoteID);
                if (templines != null)
                {
                    for (int h = 0; h < templines.Length; ++h)
                    {
                        line = new DA.Models.MovieDatabase.Line();
                        line.MovieID = Program.tempmovie.MovieID;
                        line.QuoteID = quoteid;
                        line.IMDBID = templines[h].IMDBID;
                        line.CharacterName = templines[h].CharacterName;
                        line.LineText = templines[h].LineText;
                        line.LineOrdinal = templines[h].LineOrdinal;

                        successful = false;
                        do
                        {
                            try
                            {
                                Program.BLL_Lines.Insert(ref line);
                                successful = Program.BLL_Lines.Contains(Program.tempmovie.MovieID, line.CharacterName, line.LineText);
                            }
                            catch (Exception ex)
                            {
                                successful = false;
                            }
                        } while (!successful);
                    }
                }
            }
        }

        /// <summary>
        /// Final save of Goofs using SQL joins and updates.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveGoofs(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            do
            {
                try
                {
                    sSql = "DELETE FROM [dbo].[Goofs] WHERE MovieID = " + movieid.ToString();
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);

            successful = false;
            do
            {
                try
                {
                    sSql = "INSERT INTO [Goofs] ([MovieID], [Type], [GoofText], [Ordinal]) ";
                    sSql = sSql + "SELECT [MovieID], [Type], [GoofText], [Ordinal] ";
                    sSql = sSql + "FROM[TempGoofs] ";
                    sSql = sSql + "ORDER BY[TempGoofs].[Ordinal] ";
                    _Cn.Open();
                    _Cmd = new SqlCommand(sSql, _Cn);
                    _Cmd.ExecuteNonQuery();
                    _Cn.Close();
                    successful = true;
                }
                catch (Exception ex)
                {
                    successful = false;
                }
            } while (!successful);
        }

        /// <summary>
        /// Final save of Keywords using SQL joins and updates.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveKeywords(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            DA.Models.MovieDatabase.Keyword keyword = null;
            DA.Models.MovieDatabase.KeywordList keywordlist = null;
            long keywordid = -1;
            for (int a = 0; a < Program.tempmovie.Keywords.Count; ++a)
            {
                // First, check to make sure the keyword is in KeywordLists
                if (Program.BLL_KeywordList.Contains(Program.tempmovie.Keywords[a]))
                {
                    // The keyword is in KeywordList.  Look it up and get the ID.
                    keywordlist = null;
                    do
                    {
                        keywordlist = Program.BLL_KeywordList.SelectByKeyword_model(Program.tempmovie.Keywords[a]);
                    } while (keywordlist == null);
                }
                else
                {
                    // The keyword is NOT in KeywordList.  Add it and get the ID.
                    keywordlist = new DA.Models.MovieDatabase.KeywordList();
                    keywordlist.KeywordID = -1;
                    keywordlist.Keyword = Program.tempmovie.Keywords[a];

                    successful = false;
                    do
                    {
                        try
                        {
                            keywordid = Program.BLL_KeywordList.Insert(ref keywordlist);
                            successful = Program.BLL_KeywordList.Contains(keywordlist.Keyword);
                        }
                        catch (Exception ex)
                        {
                            successful = false;
                        }
                    } while (!successful);
                    keywordlist.KeywordID = keywordid;
                }

                // Now that we have a KeywordID, add the keyword to the
                // Keywords table.
                bool InDatabase = Program.BLL_Keywords.Contains(Program.tempmovie.MovieID, keywordlist.KeywordID);
                if (InDatabase)
                {
                    do
                    {
                        keyword = Program.BLL_Keywords.SelectByMovieIDKeywordID_model(Program.tempmovie.MovieID, keywordlist.KeywordID);
                    } while (keyword == null);

                    keyword.MovieID = Program.tempmovie.MovieID;
                    keyword.KeywordID = keywordlist.KeywordID;

                    successful = false;
                    do
                    {
                        try
                        {
                            Program.BLL_Keywords.Update(ref keyword);
                            successful = true;
                        }
                        catch (Exception ex)
                        {
                            successful = false;
                        }
                    } while (!successful);
                }
                else
                {
                    keyword = new DA.Models.MovieDatabase.Keyword();
                    keyword.MovieID = Program.tempmovie.MovieID;
                    keyword.KeywordID = keywordlist.KeywordID;

                    successful = false;
                    do
                    {
                        try
                        {
                            Program.BLL_Keywords.Insert(ref keyword);
                            successful = Program.BLL_Keywords.Contains(Program.tempmovie.MovieID, keywordlist.KeywordID);
                        }
                        catch (Exception ex)
                        {
                            successful = false;
                        }
                    } while (!successful);
                }
            }
        }

        /// <summary>
        /// Final save of Simple Episodes using SQL joins and updates.
        /// </summary>
        /// <param name="movieid">The MovieID of the recently added movie.</param>
        public static void Stage07_SaveSimpleEpisodes(long movieid)
        {
            SqlConnection _Cn = new SqlConnection(Connections.ConnectionStrings.MovieDatabaseConnectionString_Private);
            SqlCommand _Cmd = null;
            String sSql = String.Empty;

            bool successful = false;
            DA.Models.MovieDatabase.SimpleTVEpisode se = null;
            DA.Models.MovieDatabase.TVEpisodeQueue teq = null;
            for (int a = 0; a < Program.tempmovie.SimpleTVEpisodes.Count; ++a)
            {
                // First, add/update SimpleTVEpisodes
                bool InDatabase = Program.BLL_SimpleTVEpisodes.Contains(Program.tempmovie.SimpleTVEpisodes[a].SeriesIMDBID, Program.tempmovie.SimpleTVEpisodes[a].EpisodeTitle);
                if (InDatabase)
                {
                    do
                    {
                        se = Program.BLL_SimpleTVEpisodes.SelectBySeriesIMDBIDEpisodeTitle_model(Program.tempmovie.SimpleTVEpisodes[a].SeriesIMDBID, Program.tempmovie.SimpleTVEpisodes[a].EpisodeTitle);
                    } while (se == null);

                    se.SeriesMovieID = Program.tempmovie.SimpleTVEpisodes[a].SeriesMovieID;
                    se.SeriesIMDBID = Program.tempmovie.SimpleTVEpisodes[a].SeriesIMDBID;
                    se.SeriesTitle = Program.tempmovie.SimpleTVEpisodes[a].SeriesTitle;
                    se.EpisodeMovieID = Program.tempmovie.SimpleTVEpisodes[a].EpisodeMovieID;
                    se.EpisodeIMDBID = Program.tempmovie.SimpleTVEpisodes[a].EpisodeIMDBID;
                    se.EpisodeTitle = Program.tempmovie.SimpleTVEpisodes[a].EpisodeTitle;
                    se.Season = Program.tempmovie.SimpleTVEpisodes[a].Season;
                    se.Episode = Program.tempmovie.SimpleTVEpisodes[a].Episode;
                    se.AirDate = Program.tempmovie.SimpleTVEpisodes[a].AirDate;
                    se.Plot = Program.tempmovie.SimpleTVEpisodes[a].Plot;
                    se.IMDBPosterURL = Program.tempmovie.SimpleTVEpisodes[a].IMDBPosterURL;
                    se.PosterFilename = Program.tempmovie.SimpleTVEpisodes[a].PosterFilename;
                    se.LocalPosterPath = Program.tempmovie.SimpleTVEpisodes[a].LocalPosterPath;
                    se.WebPosterURL = Program.tempmovie.SimpleTVEpisodes[a].WebPosterURL;

                    successful = false;
                    do
                    {
                        try
                        {
                            Program.BLL_SimpleTVEpisodes.Update(ref se);
                            successful = true;
                        }
                        catch (Exception ex)
                        {
                            successful = false;
                        }
                    } while (!successful);

                    // Check if episode is in TVEpisodeQueue
                    // Now, make sure the episode is also in the
                    // TVEpisodeQueue.  Only INSERT is handled.
                    InDatabase = Program.BLL_TVEpisodeQueue.Contains(se.EpisodeIMDBID);
                    if (InDatabase == false)
                    {
                        // The episode is not in the queue.  Add it.
                        teq = new DA.Models.MovieDatabase.TVEpisodeQueue();
                        teq.Timestamp = GetTimestamp();
                        teq.SeriesMovieID = Program.tempmovie.MovieID;
                        teq.SeriesIMDBID = Program.tempmovie.SimpleTVEpisodes[a].SeriesIMDBID;
                        teq.SeriesTitle = Program.tempmovie.SimpleTVEpisodes[a].SeriesTitle;
                        teq.EpisodeIMDBID = Program.tempmovie.SimpleTVEpisodes[a].EpisodeIMDBID;
                        teq.EpisodeTitle = Program.tempmovie.SimpleTVEpisodes[a].EpisodeTitle;
                        teq.Season = Program.tempmovie.SimpleTVEpisodes[a].Season;
                        teq.Episode = Program.tempmovie.SimpleTVEpisodes[a].Episode;
                        teq.AirDate = Program.tempmovie.SimpleTVEpisodes[a].AirDate;
                        teq.Plot = Program.tempmovie.SimpleTVEpisodes[a].Plot;
                        teq.IMDBPosterURL = Program.tempmovie.SimpleTVEpisodes[a].IMDBPosterURL;
                        teq.MovieJSON = "";
                        teq.Priority = 2;

                        successful = false;
                        do
                        {
                            try
                            {
                                Program.BLL_TVEpisodeQueue.Insert(ref teq);
                                successful = Program.BLL_TVEpisodeQueue.Contains(teq.EpisodeIMDBID);
                            }
                            catch (Exception ex)
                            {
                                successful = false;
                            }
                        } while (!successful);
                    }
                }
                else
                {
                    se = new DA.Models.MovieDatabase.SimpleTVEpisode();
                    se.SeriesMovieID = Program.tempmovie.SimpleTVEpisodes[a].SeriesMovieID;
                    se.SeriesIMDBID = Program.tempmovie.SimpleTVEpisodes[a].SeriesIMDBID;
                    se.SeriesTitle = Program.tempmovie.SimpleTVEpisodes[a].SeriesTitle;
                    se.EpisodeMovieID = Program.tempmovie.SimpleTVEpisodes[a].EpisodeMovieID;
                    se.EpisodeIMDBID = Program.tempmovie.SimpleTVEpisodes[a].EpisodeIMDBID;
                    se.EpisodeTitle = Program.tempmovie.SimpleTVEpisodes[a].EpisodeTitle;
                    se.Season = Program.tempmovie.SimpleTVEpisodes[a].Season;
                    se.Episode = Program.tempmovie.SimpleTVEpisodes[a].Episode;
                    se.AirDate = Program.tempmovie.SimpleTVEpisodes[a].AirDate;
                    se.Plot = Program.tempmovie.SimpleTVEpisodes[a].Plot;
                    se.IMDBPosterURL = Program.tempmovie.SimpleTVEpisodes[a].IMDBPosterURL;
                    se.PosterFilename = Program.tempmovie.SimpleTVEpisodes[a].PosterFilename;
                    se.LocalPosterPath = Program.tempmovie.SimpleTVEpisodes[a].LocalPosterPath;
                    se.WebPosterURL = Program.tempmovie.SimpleTVEpisodes[a].WebPosterURL;

                    successful = false;
                    do
                    {
                        try
                        {
                            Program.BLL_SimpleTVEpisodes.Insert(ref se);
                            successful = Program.BLL_SimpleTVEpisodes.Contains(se.SeriesIMDBID, se.EpisodeTitle);
                        }
                        catch (Exception ex)
                        {
                            successful = false;
                        }
                    } while (!successful);

                    // Now, make sure the episode is also in the
                    // TVEpisodeQueue.  Only INSERT is handled.
                    InDatabase = Program.BLL_TVEpisodeQueue.Contains(se.EpisodeIMDBID);
                    if (InDatabase == false)
                    {
                        // The episode is not in the queue.  Add it.
                        teq = new DA.Models.MovieDatabase.TVEpisodeQueue();
                        teq.Timestamp = GetTimestamp();
                        teq.SeriesMovieID = Program.tempmovie.MovieID;
                        teq.SeriesIMDBID = Program.tempmovie.SimpleTVEpisodes[a].SeriesIMDBID;
                        teq.SeriesTitle = Program.tempmovie.SimpleTVEpisodes[a].SeriesTitle;
                        teq.EpisodeIMDBID = Program.tempmovie.SimpleTVEpisodes[a].EpisodeIMDBID;
                        teq.EpisodeTitle = Program.tempmovie.SimpleTVEpisodes[a].EpisodeTitle;
                        teq.Season = Program.tempmovie.SimpleTVEpisodes[a].Season;
                        teq.Episode = Program.tempmovie.SimpleTVEpisodes[a].Episode;
                        teq.AirDate = Program.tempmovie.SimpleTVEpisodes[a].AirDate;
                        teq.Plot = Program.tempmovie.SimpleTVEpisodes[a].Plot;
                        teq.IMDBPosterURL = Program.tempmovie.SimpleTVEpisodes[a].IMDBPosterURL;
                        teq.MovieJSON = "";
                        teq.Priority = 2;

                        successful = false;
                        do
                        {
                            try
                            {
                                Program.BLL_TVEpisodeQueue.Insert(ref teq);
                                successful = Program.BLL_TVEpisodeQueue.Contains(teq.EpisodeIMDBID);
                            }
                            catch (Exception ex)
                            {
                                successful = false;
                            }
                        } while (!successful);
                    }
                }
            }
        }

        #endregion

        #region Stage08
        public void Stage08_FinalizeAndReset()
        {
            if (Program.tempmovie == null)
            {
                BuildTempMovieFromTempTables();
            }

            Console.Title = "Stage 8 - " + Program.tempmovie.Title;

            Console.WriteLine(DateTime.Now.ToString() + " - Movie processed.  Reset beginning...");

            // Delete movie from queues by IMDBID
            Program.BLL_MovieQueue.DeleteByIMDBID(Program.tempmovie.IMDBID);
            Program.BLL_TVEpisodeQueue.DeleteByEpisodeIMDBID(Program.tempmovie.IMDBID);

            // Truncate all temp tables.
            Program.BLL_TempMovieStatus.TruncateAll();

            // Reset TempMovieStatus
            // Set Stage = 0 and MovieSource = ""
            Program.tempmoviestatus.Stage = 0;
            Program.tempmoviestatus.MovieSource = "";

            UpdateTempMovieStatus(Program.tempmoviestatus);

            Program.tempmovie = null;
            Program.movie = null;
            Console.WriteLine(DateTime.Now.ToString() + " - Reset complete.");
            Console.WriteLine("******************************");
        }

        #endregion

        #region Utility Functions

        /// <summary>
        /// This subroutine updates the TempMovieStatus table of the database with the data contained
        /// in the given DA.Models.MovieDatabase.TempMovieStatus object and verifies the save by
        /// reloading the global object and comparing the data with the input parameter.
        /// </summary>
        /// <param name="tempmoviestatus">The source object.</param>
        public static void UpdateTempMovieStatus(DA.Models.MovieDatabase.TempMovieStatus tempmoviestatus)
        {
            DA.Models.MovieDatabase.TempMovieStatus verify_tms = tempmoviestatus;
            bool success = false;

            do
            {
                try
                {
                    Program.BLL_TempMovieStatus.Update(ref Program.tempmoviestatus);
                    Program.tempmoviestatus = Program.BLL_TempMovieStatus.SelectByID_model(1);
                    success = ((Program.tempmoviestatus.Stage == verify_tms.Stage) && (Program.tempmoviestatus.MovieSource.Equals(verify_tms.MovieSource)));
                }
                catch
                {
                    success = false;
                }
            } while (success == false);
        }

        /// <summary>
        /// If the application was interrupted in stage 1 or stage 2, it will
        /// not pick back up properly.  Reset the stage to 0.
        /// </summary>
        public void CheckStartingState()
        {
            bool QuerySuccess = false;

            if ((Program.tempmoviestatus.Stage == 1) || (Program.tempmoviestatus.Stage == 2))
            {
                Program.tempmoviestatus.Stage = 0;
                Program.tempmoviestatus.MovieSource = "";

                UpdateTempMovieStatus(Program.tempmoviestatus);
            }
        }

        /// <summary>
        /// This subroutine adds a record in the MovieQueue_Errors table with the data in the given
        /// object and the given reason.
        /// </summary>
        /// <param name="moviequeue">The MovieQueue object containing the data.</param>
        /// <param name="reason">The reason for the error.</param>
        public static void AddToMovieQueueErrors(DA.Models.MovieDatabase.MovieQueue moviequeue, String type, String errordescription)
        {
            DA.Models.MovieDatabase.MovieQueue_Error moviequeueerror = new DA.Models.MovieDatabase.MovieQueue_Error();
            moviequeueerror.Timestamp = GetTimestamp();
            moviequeueerror.IMDBID = moviequeue.IMDBID;
            moviequeueerror.Title = moviequeue.Title;
            moviequeueerror.Year = moviequeue.Year;
            moviequeueerror.Remarks = "";
            moviequeueerror.Type = type;
            moviequeueerror.MovieJSON = moviequeue.MovieJSON;
            moviequeueerror.Priority = 3;
            moviequeueerror.DateOfError = GetTimestamp();
            moviequeueerror.ErrorDescription = errordescription;

            bool success = false;
            do
            {
                try
                {
                    Program.BLL_MovieQueue_Errors.Insert(ref moviequeueerror);
                    success = Program.BLL_MovieQueue_Errors.Contains(moviequeueerror.IMDBID);
                }
                catch (Exception ex)
                {
                    success = false;
                }
            } while (!success);
        }

        /// <summary>
        /// This subroutine adds a record in the MovieQueue_Errors table with the data in the given
        /// object and the given reason.
        /// </summary>
        /// <param name="moviequeue">The MovieQueue object containing the data.</param>
        /// <param name="reason">The reason for the error.</param>
        public static void AddToTVEpisodeQueueErrors(String errordescription)
        {
            DA.Models.MovieDatabase.TVEpisodeQueue_Error tvepisodequeueerror = new DA.Models.MovieDatabase.TVEpisodeQueue_Error();

            if (!Program.BLL_TVEpisodeQueue_Errors.Contains(Program.tempmovie.IMDBID))
            {
                tvepisodequeueerror.QueueID = -1;
                tvepisodequeueerror.Timestamp = GetTimestamp();
                tvepisodequeueerror.SeriesMovieID = -1;
                tvepisodequeueerror.SeriesIMDBID = Program.tempmovie.SeriesIMDBID;
                tvepisodequeueerror.SeriesTitle = Program.tempmovie.SeriesName;
                tvepisodequeueerror.EpisodeIMDBID = Program.tempmovie.IMDBID;
                tvepisodequeueerror.EpisodeTitle = Program.tempmovie.Title;
                tvepisodequeueerror.Season = "";
                tvepisodequeueerror.Episode = "";
                tvepisodequeueerror.AirDate = Program.tempmovie.ReleaseDate;
                tvepisodequeueerror.Plot = Program.tempmovie.Plot;
                tvepisodequeueerror.IMDBPosterURL = Program.tempmovie.IMDBPosterURL;
                tvepisodequeueerror.MovieJSON = Program.tempmovie.MovieJSON;
                tvepisodequeueerror.Priority = 3;
                tvepisodequeueerror.DateOfError = GetTimestamp();
                tvepisodequeueerror.ErrorDescription = errordescription;

                bool successful = false;
                do
                {
                    try
                    {
                        Program.BLL_TVEpisodeQueue_Errors.Insert(ref tvepisodequeueerror);
                        successful = Program.BLL_TVEpisodeQueue_Errors.Contains(tvepisodequeueerror.EpisodeIMDBID);
                    }
                    catch
                    {
                        successful = false;
                    }
                } while (!successful);
            }
        }

        /// <summary>
        /// This subroutine deletes a movie from the Movie Queue by its IMDBID.
        /// </summary>
        /// <param name="imdbid">The IMDBID of the movie to be deleted from the Movie Queue.</param>
        public static void DeleteFromMovieQueue(String imdbid)
        {
            bool success = false;
            do
            {
                try
                {
                    Program.BLL_MovieQueue.DeleteByIMDBID(imdbid);
                    success = (!Program.BLL_MovieQueue.Contains(Program.tempmovie.IMDBID));
                }
                catch (Exception ex)
                {
                    success = false;
                }
            } while (!success);
        }

        /// <summary>
        /// This subroutine deletes a movie from the TV Episode Queue by its IMDBID.
        /// </summary>
        /// <param name="imdbid">The IMDBID of the movie to be deleted from the TV Episode Queue.</param>
        public static void DeleteFromTVEpisodeQueue(String imdbid)
        {
            bool success = false;
            do
            {
                try
                {
                    Program.BLL_TVEpisodeQueue.DeleteByEpisodeIMDBID(imdbid);
                    success = (!Program.BLL_TVEpisodeQueue.Contains(Program.tempmovie.IMDBID));
                }
                catch (Exception ex)
                {
                    success = false;
                }
            } while (!success);
        }

        /// <summary>
        /// Converts today's date into the format YYYYMMDDHHmmSSmmm.
        /// </summary>
        /// <returns>The current date and time in the format YYYYMMDDHHmmSSmmm.</returns>
        private static String GetTimestamp()
        {
            String returnvaleue = String.Empty;
            DateTime today = DateTime.Now;
            returnvaleue = today.Year.ToString().PadLeft(4, '0') + today.Month.ToString().PadLeft(2, '0') + today.Day.ToString().PadLeft(2, '0') + today.Hour.ToString().PadLeft(2, '0') + today.Minute.ToString().PadLeft(2, '0') + today.Second.ToString().PadLeft(2, '0') + today.Millisecond.ToString().PadLeft(3, '0');
            return returnvaleue;
        }



        private static bool FTPUploadFile(String filetoupload, String ftpuri, String ftpusername, String ftppassword)
        {
            bool success = false;

            // Create a web request that will be used to talk with the server and set the request method to upload a file by ftp.
            FtpWebRequest ftpRequest = (FtpWebRequest)WebRequest.Create(ftpuri);

            try
            {
                ftpRequest.Method = WebRequestMethods.Ftp.UploadFile;

                // Confirm the Network credentials based on the user name and password passed in.
                ftpRequest.Credentials = new NetworkCredential(ftpusername, ftppassword);

                // Read into a Byte array the contents of the file to be uploaded 
                Byte[] bytes = System.IO.File.ReadAllBytes(filetoupload);

                // Transfer the byte array contents into the request stream, write and then close when done.
                ftpRequest.ContentLength = bytes.Length;

                using (Stream UploadStream = ftpRequest.GetRequestStream())
                {
                    UploadStream.Write(bytes, 0, bytes.Length);
                    UploadStream.Close();
                }
                success = true;
            }
            catch
            {
                success = false;
            }

            return success;
        }

        private static bool FTPDownloadFile(String downloadpath, String ftpuri, String ftpusername, String ftppassword)
        {
            bool success = false;

            // Create a WebClient.
            WebClient request = new WebClient();

            // Confirm the Network credentials based on the user name and password passed in.
            request.Credentials = new NetworkCredential(ftpusername, ftppassword);

            // Read the file data into a Byte array
            Byte[] bytes = request.DownloadData(ftpuri);

            try
            {
                // Create a FileStream to read the file into
                FileStream DownloadStream = System.IO.File.Create(downloadpath);

                // Stream this data into the file
                DownloadStream.Write(bytes, 0, bytes.Length);

                // Close the FileStream
                DownloadStream.Close();

                success = true;
            }
            catch
            {
                success = false;
            }

            return success;
        }

        private static byte[] ConvertFileToByteArray(String filepath)
        {
            byte[] file;
            using (var stream = new FileStream(filepath, FileMode.Open, FileAccess.Read))
            {
                using (var reader = new BinaryReader(stream))
                {
                    file = reader.ReadBytes((int)stream.Length);
                }
            }
            return file;
        }

        private static String StripInvalidCharactersFromFilename(String originalfilename)
        {
            String r = originalfilename;
            r = r.Replace("<", "");
            r = r.Replace(">", "");
            r = r.Replace(":", "");
            r = r.Replace("\"", "");
            r = r.Replace("/", "");
            r = r.Replace("\\", "");
            r = r.Replace("|", "");
            r = r.Replace("?", "");
            r = r.Replace("*", "");

            return r;
        }

        #endregion


    }
}
