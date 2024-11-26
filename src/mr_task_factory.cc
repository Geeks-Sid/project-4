/* DON'T MAKE ANY CHANGES IN THIS FILE */

#include <utility>
#include <unordered_map>
#include <functional>
#include "mr_tasks.h"
#include <mr_task_factory.h>

// Constructor for BaseMapper, it initializes the internal implementation.
BaseMapper::BaseMapper() : impl_(new BaseMapperInternal) {}

// Destructor for BaseMapper.
BaseMapper::~BaseMapper() {}

// Emits a key-value pair for the mapper.
void BaseMapper::emit(const std::string &key, const std::string &val)
{
	impl_->emit(key, val);
}

// Constructor for BaseReducer, it initializes the internal implementation.
BaseReducer::BaseReducer() : impl_(new BaseReducerInternal) {}

// Destructor for BaseReducer.
BaseReducer::~BaseReducer() {}

// Emits a key-value pair for the reducer.
void BaseReducer::emit(const std::string &key, const std::string &val)
{
	impl_->emit(key, val);
}

namespace
{

	/**
	 * @class TaskFactory
	 * @brief Singleton factory class for creating and managing mappers and reducers.
	 */
	class TaskFactory
	{
	public:
		/**
		 * @brief Provides access to the singleton instance of TaskFactory.
		 * @return Reference to the singleton TaskFactory instance.
		 */
		static TaskFactory &instance();

		/**
		 * @brief Retrieves a mapper associated with a specific user ID.
		 * @param user_id The user ID for which the mapper is requested.
		 * @return Shared pointer to the BaseMapper object, or nullptr if not found.
		 */
		std::shared_ptr<BaseMapper> get_mapper(const std::string &user_id);

		/**
		 * @brief Retrieves a reducer associated with a specific user ID.
		 * @param user_id The user ID for which the reducer is requested.
		 * @return Shared pointer to the BaseReducer object, or nullptr if not found.
		 */
		std::shared_ptr<BaseReducer> get_reducer(const std::string &user_id);

		// Maps user IDs to mapper and reducer generator functions.
		std::unordered_map<std::string, std::function<std::shared_ptr<BaseMapper>()>> mappers_;
		std::unordered_map<std::string, std::function<std::shared_ptr<BaseReducer>()>> reducers_;

	private:
		// Private constructor to enforce the singleton pattern.
		TaskFactory();
	};

	TaskFactory &TaskFactory::instance()
	{
		// Static instance of TaskFactory, ensuring a single instance is used.
		static TaskFactory *instance = new TaskFactory();
		return *instance;
	}

	// Constructor for TaskFactory.
	TaskFactory::TaskFactory() {}

	std::shared_ptr<BaseMapper> TaskFactory::get_mapper(const std::string &user_id)
	{
		auto itr = mappers_.find(user_id);
		if (itr == mappers_.end())
			return nullptr;	  // Return nullptr if no mapper is found for the user ID.
		return itr->second(); // Return the mapper associated with the user ID.
	}

	std::shared_ptr<BaseReducer> TaskFactory::get_reducer(const std::string &user_id)
	{
		auto itr = reducers_.find(user_id);
		if (itr == reducers_.end())
			return nullptr;	  // Return nullptr if no reducer is found for the user ID.
		return itr->second(); // Return the reducer associated with the user ID.
	}
}

/**
 * @brief Registers mapper and reducer generator functions for a specific user ID.
 * @param user_id The user ID for which the tasks are registered.
 * @param generate_mapper Function to generate a BaseMapper object.
 * @param generate_reducer Function to generate a BaseReducer object.
 * @return True if both mapper and reducer are successfully registered, false otherwise.
 */
bool register_tasks(std::string user_id, std::function<std::shared_ptr<BaseMapper>()> &generate_mapper,
					std::function<std::shared_ptr<BaseReducer>()> &generate_reducer)
{
	TaskFactory &factory = TaskFactory::instance();
	return factory.mappers_.insert(std::make_pair(user_id, generate_mapper)).second &&
		   factory.reducers_.insert(std::make_pair(user_id, generate_reducer)).second;
}

/**
 * @brief Retrieves a mapper from the TaskFactory for a specific user ID.
 * @param user_id The user ID for which the mapper is requested.
 * @return Shared pointer to the BaseMapper object, or nullptr if not found.
 */
std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string &user_id)
{
	return TaskFactory::instance().get_mapper(user_id);
}

/**
 * @brief Retrieves a reducer from the TaskFactory for a specific user ID.
 * @param user_id The user ID for which the reducer is requested.
 * @return Shared pointer to the BaseReducer object, or nullptr if not found.
 */
std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string &user_id)
{
	return TaskFactory::instance().get_reducer(user_id);
}
