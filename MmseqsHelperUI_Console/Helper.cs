﻿using System.Linq;
using System.Text.Json;
using Microsoft.Extensions.Configuration;

namespace MmseqsHelperUI_Console;

internal static class Helper
{
    public static string GetConfigurationString(IConfiguration config)
    {
        return String.Join("\n", config.GetChildren().Select(GetAllKeyValuePairsNested));
    }

    private const char JsonIndentSymbol = ' ';

    public static string GetConfigJsonFromConfig(IConfiguration config)
    {
        return "{\n" + String.Join(",\n", config.GetChildren().Select(x=>GetJsonSectionsNested(x,1))) + "\n}";
    }

    private static string GetJsonSectionsNested(IConfigurationSection configurationSection, int level)
    {
        var indent = new string(JsonIndentSymbol, level);

        if (!configurationSection.Exists()) return String.Empty;
        if (configurationSection.GetChildren()?.Any() == true)
        {
            var nestedEntries = configurationSection.GetChildren().Select(x=>GetJsonSectionsNested(x,level+1));
            var jsonFormatted = indent +  $"{Jsonize(configurationSection.Key)}" + ": {\n" + String.Join(",\n", nestedEntries) + "\n" + indent + "}";
            return jsonFormatted;
        }

        return indent + $"{Jsonize(configurationSection.Key)}: {Jsonize(configurationSection.Value)}";

    }

    private static string Jsonize(string inString)
    {
        return "\"" +  inString.Replace("\\","\\\\").Replace("\"","\\\"") + "\"";
    }

    private static string GetAllKeyValuePairsNested(IConfigurationSection configurationSection)
    {
        if (!configurationSection.Exists()) return String.Empty;
        if (configurationSection.GetChildren()?.Any() == true) return String.Join("\n", configurationSection.GetChildren().Select(GetAllKeyValuePairsNested));
        return $"{configurationSection.Path} :: {configurationSection.Value}";

    }

    public static string GetConfigJsonFromDefaults(Dictionary<string, (string preset, bool required)> defaults)
    {
        var values = String.Join(",\n", defaults.Select(x => $"{Jsonize(x.Key)} : {Jsonize(x.Value.preset)}"));

        return "{\n" + values + "\n}";
    }
}