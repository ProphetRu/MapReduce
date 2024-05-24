#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <filesystem>
#include <thread>
#include <functional>
#include <map>
#include <list>
#include <ranges>
#include <stdexcept>

/**
 * @brief A function type for the map function in MapReduce.
 * @param line The input line to be mapped.
 * @return A list of strings representing the mapped output.
 */
using MapFunction = std::function<std::list<std::string>(const std::string&)>;

/**
 * @brief A function type for the reduce function in MapReduce.
 * @param list The list of strings to be reduced.
 * @return A list of strings representing the reduced output.
 */
using ReduceFunction = std::function<std::list<std::string>(const std::list<std::string>&)>;

/**
 * @brief Splits the input file into sections based on the specified number of sections.
 * @param input_file_path The path to the input file.
 * @param num_sections The number of sections to split the file into.
 * @return A vector of stream positions representing the start positions of each section.
 */
std::vector<std::streampos> SplitFileIntoSections(const std::string& input_file_path, int num_sections)
{
    // Helper function to get the size of the file
    auto get_file_size = [](std::ifstream& file) noexcept
    {
        file.seekg(0, std::ios::end);
        const auto file_size{ file.tellg() };
        file.seekg(0, std::ios::beg);
        return file_size;
    };

    // Validate input arguments
    if (input_file_path.empty() || num_sections <= 0)
    {
        throw std::invalid_argument("Invalid argument");
    }

    // Open the input file
    std::ifstream input_file(input_file_path);
    if (!input_file.is_open())
    {
        throw std::runtime_error("Can't open file");
    }

    // Get the size of the file
    const auto file_size{ get_file_size(input_file) };
    if (file_size <= 0)
    {
        throw std::runtime_error("File is empty");
    }

    // Calculate the size of each section
    const auto section_size{ file_size / num_sections };
    std::vector<std::streampos> section_start_positions(num_sections + 1);
    section_start_positions[0] = 0;

    // Find the start positions of each section
    for (auto i{ 1 }; i < num_sections; ++i) 
    {
        input_file.seekg(section_size * i);
        std::string line;
        std::getline(input_file, line);
        section_start_positions[i] = input_file.tellg();
    }
    section_start_positions[num_sections] = file_size;

    // Close the input file and return the section start positions
    input_file.close();
    return section_start_positions;
}

/**
 * @brief Maps a section of the input file using the specified map function.
 * @param input_file_path The path to the input file.
 * @param start_pos The start position of the section to map.
 * @param end_pos The end position of the section to map.
 * @param map_func The map function to apply to each line in the section.
 * @param output_list The list to store the mapped output.
 */
void MapSection(const std::string& input_file_path, std::streampos start_pos, std::streampos end_pos, const MapFunction& map_func, std::list<std::string>& output_list)
{
    // Validate input arguments
    if (input_file_path.empty() || !map_func)
    {
        throw std::invalid_argument("Invalid argument");
    }

    // Open the input file
    std::ifstream input_file(input_file_path);
    if (!input_file.is_open()) 
    {
        throw std::runtime_error("Can't open file");
    }

    // Map each line in the section
    input_file.seekg(start_pos);
    std::string line;
    while (input_file.tellg() < end_pos && std::getline(input_file, line)) 
    {
        std::list<std::string> mapped = map_func(line);
        output_list.insert(end(output_list), begin(mapped), end(mapped));
    }

    // Close the input file
    input_file.close();
}

/**
 * @brief Shuffles the map results into the specified number of reducers.
 * @param map_results The vector of map result lists.
 * @param num_reducers The number of reducers to shuffle the results into.
 * @return A vector of shuffled result lists, one for each reducer.
 */
std::vector<std::list<std::string>> Shuffle(const std::vector<std::list<std::string>>& map_results, int num_reducers)
{
    // Validate input arguments
    if (map_results.empty() || num_reducers <= 0)
    {
        throw std::invalid_argument("Invalid argument");
    }

    // Group the map results by key
    std::map<std::string, std::list<std::string>> grouped_results;
    for (const auto& resultList : map_results) 
    {
        for (const auto& item : resultList) 
        {
            grouped_results[item].push_back(item);
        }
    }

    // Distribute the grouped results into the reducers
    std::vector<std::list<std::string>> shuffle_results(num_reducers);
    int i = 0;

    for (const auto& group : grouped_results | std::views::values) 
    {
        shuffle_results[i % num_reducers].insert(shuffle_results[i % num_reducers].end(), group.begin(), group.end());
        ++i;
    }

    return shuffle_results;
}

/**
 * @brief Reduces the input list using the specified reduce function and writes the result to an output file.
 * @param input_list The list of strings to be reduced.
 * @param reduce_func The reduce function to apply to the input list.
 * @param output_file_name The name of the output file to write the reduced result to.
 */
void ReduceContainer(const std::list<std::string>& input_list, const ReduceFunction& reduce_func, const std::string& output_file_name)
{
    // Validate input arguments
    if (input_list.empty() || !reduce_func || output_file_name.empty())
    {
        throw std::invalid_argument("Invalid argument");
    }

    // Reduce the input list
    const auto reduced{ reduce_func(input_list) };

    // Open the output file
    std::ofstream output_file(output_file_name);
    if (!output_file.is_open()) 
    {
        throw std::runtime_error("Can't open file");
    }

    // Write the reduced result to the output file
    for (const auto& item : reduced) 
    {
        output_file << item << std::endl;
    }

    // Close the output file
    output_file.close();
}

int main(int argc, char* argv[]) noexcept
{
    // Check if the correct number of arguments are provided
    if (argc != 4)
    {
        std::cout << "Usage: " << argv[0] << " <src> <mnum> <runm>\n";
        return -1;
    }

    try
    {
        // Get the input file path, number of mappers, and number of reducers from the command line arguments
        const auto file_path{ argv[1] };
        const auto mnum{ std::atoi(argv[2]) };
        const auto rnum{ std::atoi(argv[3]) };

        // Split the input file into sections
        const auto section_start_positions{ SplitFileIntoSections(file_path, mnum) };

        // Map each section using worker threads
        std::vector<std::thread> worker_threads;
        std::vector<std::list<std::string>> map_results(mnum);

        // Define the map function
        auto map_func = [](const std::string& line) noexcept
    	{
            return std::list{ line };
        };

        for (auto i{ 0 }; i < mnum; ++i)
        {
            worker_threads.emplace_back(MapSection, file_path, section_start_positions[i], section_start_positions[i + 1], std::cref(map_func), std::ref(map_results[i]));
        }

        // Wait for all worker threads to finish
        for (auto& thread : worker_threads) 
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
        worker_threads.clear();

        // Shuffle the map results into the reducers
        const auto shuffle_results{ Shuffle(map_results, rnum) };

        // Reduce each shuffled result list using worker threads
        auto reduce_func = [](const std::list<std::string>& list) noexcept
    	{
            return list;
        };

        for (auto i{ 0 }; i < rnum; ++i)
        {
            const std::string output_file_name = "output_" + std::to_string(i) + ".txt";
            worker_threads.emplace_back(ReduceContainer, std::cref(shuffle_results[i]), std::cref(reduce_func), output_file_name);
        }

        // Wait for all worker threads to finish
        for (auto& thread : worker_threads) 
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
    }
    catch (const std::exception& ex)
    {
        std::cerr << ex.what() << std::endl;
    }

	return 0;
}
