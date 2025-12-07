package edu.uchicago.quake;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
public class QuakeController {

    private final HBaseService hBaseService;

    @Autowired
    public QuakeController(HBaseService hBaseService) {
        this.hBaseService = hBaseService;
    }

    @GetMapping("/")
    public String index() {
        return "index"; // templates/index.html
    }

    @GetMapping("/search")
    public String search(
            @RequestParam("state") String state,
            @RequestParam("year") int year,
            @RequestParam("month") int month,
            Model model) {

        try {
            QuakeRecord record = hBaseService.getQuakeRecord(state, year, month);
            model.addAttribute("state", state.toUpperCase());
            model.addAttribute("year", year);
            model.addAttribute("month", month);

            if (record == null) {
                model.addAttribute("notFound", true);
            } else {
                model.addAttribute("record", record);
            }
        } catch (Exception e) {
            model.addAttribute("error", e.getMessage());
        }

        return "index";
    }
}