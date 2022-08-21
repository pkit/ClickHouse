#include "ExecutableSettings.h"

#include <Common/Exception.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(ExecutableSettingsTraits, LIST_OF_EXECUTABLE_SETTINGS)

void ExecutableSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

void ExecutableSettings::applyEnvVars(const SettingsChanges & changes, const std::vector<String> & allow_list)
{
    for (auto change : changes)
    {
        for (const auto & allowed : allow_list)
        {
            re2::RE2 matcher(allowed);
            if (re2::RE2::FullMatch(change.name, matcher))
            {
                env_vars.emplace_back(std::move(change.name));
                // env var values are always strings
                env_vars.emplace_back(DB::toString(change.value));
                break;
            }
        }
    }
}

}
