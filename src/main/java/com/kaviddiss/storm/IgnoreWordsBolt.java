package com.kaviddiss.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Bolt filters out a predefined set of words.
 * @author davidk
 */
public class IgnoreWordsBolt extends BaseRichBolt {

    private Set<String> IGNORE_LIST = new HashSet<String>(Arrays.asList(new String[] {
           "message","weather", "never", "after", "there", "today", "channel", "updates", "update", "channel", "september",
            "aiming", "really", "please", "makes", "make", "please", "follow", "world", "first", "always",
            "?????????", "???????","?", "??", "???", "????", "?????", "??????","https", "http", "the", "you", "que",
            "and", "for", "that", "like", "have", "this", "just", "with", "all", "get", "about", "money", "their",
            "expect", "http?", "before", "@rzzp", "eurekamag", "for", "doesn",
            "can", "was", "not", "your", "but", "are", "one", "what", "out", "when", "get", "lol", "now", "para", "por",
            "want", "will", "know", "good", "from", "las", "don", "people", "got", "why", "con", "time", "would", "af",
            "alle", "andet", "andre", "at", "begge", "da", "de", "den", "denne", "der", "deres", "det", "dette",
            "dig", "din", "dog", "du", "ej", "eller", "en", "end", "ene", "eneste", "enhver", "et", "fem", "fire", "flere",
            "fleste", "for", "fordi", "forrige fra", "få", "før", "god", "han", "hans", "har", "hendes", "her", "hun", "hvad",
            "hvem", "hver", "hvilken", "hvis", "hvor", "hvordan", "hvorfor", "hvornår", "i", "ikke", "ind", "ingen", "intet",
            "jeg", "jeres", "kan", "kom", "kommer", "lav", "lidt", "lille", "man mand", "mange", "med", "meget", "men", "mens",
            "mere", "mig", "ned", "ni", "nogen", "noget", "ny", "nyt", "nær", "næste", "næsten", "og", "op", "otte", "over", "på",
            "se", "seks", "ses", "som", "stor", "store", "syv", "ti", "til", "to", "tre", "ud", "var", "ad", "af", "afgjort",
            "aldrig", "alene", "alle", "allerede", "alligevel", "alt", "altid", "alvorlig", "alvorligt", "anden", "andet", "andre",
            "anfør", "angivelse", "angivet", "at", "bag", "bagefter", "bare", "bedre", "bedst", "begge", "behov", "beskrevet",
            "bestemt", "betyde", "bla", "blandt", "blev", "blevet", "blive", "bliver", "blot", "brug", "bruge", "brugt", "burde",
            "bør", "da", "de", "dem", "den", "denne", "der", "derefter", "deres", "derfor", "derfra", "deri", "derved", "desuden",
            "desværre", "det", "dette", "dig", "din", "dine", "dit", "diverse", "dog", "du", "efter", "egen", "ej", "eksempel",
            "eller", "ellers", "elleve", "en", "end", "endnu", "endvidere", "ene", "eneste", "engang", "enhver", "enten", "er",
            "et", "faktisk", "fem", "femte", "fik", "fire", "fjerde", "flere", "fleste", "for", "fordi", "forhåbentlig", "forhånd",
            "formodentlig", "forrige", "forskellige", "forsøger", "forsøgt", "fortælle", "fra", "frem", "frygtelig", "fuld",
            "fulgt", "færdig", "følgende", "følger", "før", "første", "få", "fået", "får", "gang", "gerne", "gik", "give", "giver",
            "giver", "givet", "givet", "gjorde", "gjort", "god", "godt", "gone", "grundig", "grundigt", "gør", "gør", "gøre",
            "gøre", "gå", "går", "ham", "han", "hans", "har", "havde", "have", "hej", "hel", "hele", "helt", "hende", "hendes",
            "henholdsvis", "her", "herefter", "hereupon", "heri", "hermed", "hilsen", "hilsener", "hjælp", "hjælpe", "holde",
            "holder", "holdt", "hovedsageligt", "hun", "husk", "hvad", "hvem", "hver", "hverken", "hvilken", "hvis", "hvor",
            "hvordan", "hvorefter", "hvorfor", "hvorfra", "hvorhen", "hvori", "hvornår", "hvorved", "hvorvidt", "højre", "i",
            "idet", "ifølge", "igen", "igennem", "ikke", "imens", "ind", "indad", "indeholdende", "indeholder", "inden",
            "indikerer", "indre", "indtil", "ingen", "intet", "især", "ja", "jeg", "jer", "jeres", "kan", "kender", "kendt",
            "klart", "kom", "komme", "kommer", "kort", "kun", "kunne", "lade", "lagde", "lagt", "langt", "lav", "leder", "lidt",
            "lige", "lignende", "lille", "lægge", "lægger", "man", "mand", "mange", "masser", "med", "medmindre", "meget",
            "mellem", "mellemtiden", "men", "mens", "mere", "mest", "meste", "mig", "min", "mindre", "mindst", "mit", "mod",
            "muligt", "måde", "måske", "naturligvis", "navn", "ned", "nedad", "nedenfor", "nemlig", "netop", "ni", "niende",
            "nogen", "nogensinde", "noget", "nogle", "nok", "normalt", "nu", "nul", "ny", "nyt", "nyttige", "næppe", "nær",
            "nærheden", "næste", "næsten", "nødvendig", "når", "og", "også", "ok", "okay", "om", "omfang", "op", "os", "otte",
            "ottende", "ovenfor", "over", "overalt", "overveje", "overvejer", "passende", "plus", "præcis", "prøve", "på",
            "relativt", "ret", "sagde", "samlet", "samme", "sammen", "samt", "sandsynligvis", "se", "seks", "sekund", "selv",
            "selvom", "sendt", "senere", "ses", "set", "siden", "sidst", "sidste", "sidstnævnte", "sig", "sige", "siger",
            "sikker", "sin", "sit", "sjette", "skal", "sker", "snarere", "snart", "som", "sommetider", "specificere",
            "specificeret", "spekulerer", "spørger", "stadig", "stor", "store", "synes", "syntes", "syv", "syvende", "særlige",
            "særligt", "så", "sådan", "således", "tag", "tage", "taget", "tak", "ti", "tidligere", "tiende", "til", "tilgængelig",
            "tillade", "tillader", "tilsvarende", "tit", "to", "tog", "tre", "tredje", "trods", "tror", "tværs", "tænke", "uanset",
            "ud", "uden", "udenfor", "udover", "under", "undtagen", "var", "ved", "vedrørende", "vej", "velkommen", "venligst",
            "vi", "via", "vil", "ville", "villige", "virkelig", "visse", "vores", "væk", "være",
            "været", "yderligere", "linda", "besked", "venlig", "jonatan", "henrik", "hilsen", "adresse", "kontakte", "velkommen",
            "nummer", "nærmere", "kigge", "martin", "facebook", "henvendelse", "undersøge", "undersøge", "telefonnummer", "privatbesked",
            "skrive", "tilbage", "sende", "forstå", "privat", "bedste", "andreas", "janice", "hilsner", "skulle", "ringe", "while",
            "mathias", "selvfølgelig", "stefan"
    }));
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String lang = (String) input.getValueByField("lang");
        String word = (String) input.getValueByField("word");
        if (!IGNORE_LIST.contains(word)) {
            collector.emit(new Values(lang, word));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("lang", "word"));
    }
}
